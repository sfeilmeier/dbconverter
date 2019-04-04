package dbconverter.influx;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Point.Builder;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.dto.QueryResult.Result;
import org.influxdb.dto.QueryResult.Series;

import dbconverter.Settings;
import dbconverter.Utils.Things;

public class Influx {

	public static QueryResult query(int femsId, ZonedDateTime fromDate, ZonedDateTime toDate, String measurement,
			Set<String> channels) {
		// Prepare query string
		StringBuilder query = new StringBuilder("SELECT ");
		query.append(toChannelAddressList(channels));
		query.append(" FROM " + measurement + " WHERE ");
		query.append("fems = '" + femsId + "' AND ");
		query.append("time > ");
		query.append(String.valueOf(fromDate.toEpochSecond()));
		query.append("s");
		query.append(" AND time < ");
		query.append(String.valueOf(toDate.toEpochSecond()));
		query.append("s");

		return query(query.toString());
	}

	public static QueryResult query(String query) {
		try (InfluxDB influxDB = InfluxDBFactory.connect(Settings.INFLUX_URL, Settings.INFLUX_USER,
				Settings.INFLUX_PASSWORD)) {
			QueryResult queryResult = influxDB.query(new Query(query, Settings.INFLUX_DATABASE), TimeUnit.MILLISECONDS);
			return queryResult;
		}
	}

	public static void write(BatchPoints batchPoints) {
		try (InfluxDB influxDB = InfluxDBFactory.connect(Settings.INFLUX_URL, Settings.INFLUX_USER,
				Settings.INFLUX_PASSWORD)) {
			influxDB.write(batchPoints);
		}
	}

	private static String toChannelAddressList(Set<String> channels) {
		ArrayList<String> result = new ArrayList<>();
		for (String channel : channels) {
			result.add("\"" + channel + "\" AS \"" + channel + "\"");
		}
		return String.join(", ", result);
	}

	@FunctionalInterface
	public static interface PointsFunction {
		public Map<String, Object> apply(Things things, Map<String, Object> input) throws Exception;
	}

	public static Map<Long, Map<String, Object>> queryResultToList(QueryResult... queryResults) {
		Map<Long, Map<String, Object>> result = new HashMap<>();
		for (QueryResult queryResult : queryResults) {
			for (Result r : queryResult.getResults()) {
				List<Series> seriess = r.getSeries();
				if (seriess != null) {
					for (Series series : seriess) {
						List<String> columns = series.getColumns();
						for (List<Object> values : series.getValues()) {
							Map<String, Object> fields = new HashMap<>();
							for (int i = 0; i < values.size(); i++) {
								Object value = values.get(i);
								if (value != null) {
									fields.put(columns.get(i), value);
								}
							}

							Long timestamp = Long.valueOf((long) ((Double) values.get(0)).doubleValue());
							Map<String, Object> existingFields = result.get(timestamp);
							if (existingFields == null) {
								result.put(timestamp, fields);
							} else {
								existingFields.putAll(fields);
							}
						}
					}
				}
			}
		}
		return result;
	}

	public static BatchPoints createBatchPoints(int femsId, Things things, Map<Long, Map<String, Object>> data,
			PointsFunction function) throws Exception {
		// count number of points
		int noOfPoints = 0;

		// initialize BatchPoints
		BatchPoints batchPoints = BatchPoints.database(Settings.INFLUX_DATABASE) //
				.tag("fems", String.valueOf(femsId)) //
				.build();

		// parse QueryResult
		for (Entry<Long, Map<String, Object>> entry : data.entrySet()) {
			long timestamp = entry.getKey();
			// use helper method to create Points that should be written to database
			Point point = createPoint(things, timestamp, entry.getValue(), function);
			if (point != null) {
				noOfPoints++;
				batchPoints.point(point);
			}
		}

		// info output
		System.out.println("  Number of Points: " + noOfPoints);

		// No Points? return null
		if (noOfPoints == 0) {
			return null;
		}

		return batchPoints;
	}

	private static Point createPoint(Things things, long timestamp, Map<String, Object> input, PointsFunction function)
			throws Exception {
		Settings settings = new Settings();

		// run function
		Map<String, Object> output = function.apply(things, input);
		// stop on empty output
		if (output.isEmpty()) {
			return null;
		}
		// create point
		Builder point = Point.measurement(settings.INFLUX_TARGET_MEASUREMENT).time(timestamp, TimeUnit.MILLISECONDS)
				.fields(output);
		return point.build();
	}
}
