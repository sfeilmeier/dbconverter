package dbconverter;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

import org.influxdb.InfluxDBIOException;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.QueryResult;

import dbconverter.Settings.Types;
import dbconverter.Utils.Things;
import dbconverter.influx.Influx;
import dbconverter.odoo.Odoo;

public class App {

	private final static int FEMS = 730;
//	public final static Types TYPE = Types.DESS;
	public final static Types TYPE = Types.OPENEMS_V1;

//	private final static String FROM_DATE = "2018-10-29T00:00:00";
	private final static String FROM_DATE = "";
//	private final static String TO_DATE = "2018-10-30T00:00:00";
	private final static String TO_DATE = "";

	public final static boolean OVERWRITE = false;

	public static void main(String[] args) throws Exception {
		Converter converter = new Converter();
		Settings settings = new Settings();

		// Get configuration
		Things things = null;
		if (TYPE == Types.OPENEMS_V1) {
			EdgeConfig config = Odoo.getConfig(FEMS);
			things = Utils.getThings(config);
		}

		// Get start/end date
		ZonedDateTime initialFromDate = Utils.getFromDate(FEMS, FROM_DATE);
		ZonedDateTime initialToDate = Utils.getToDate(TO_DATE);

		List<Utils.TimeChunk> timeChunks = Utils.getTimeChunks(initialFromDate, initialToDate, Settings.CHUNK_DAYS);
		for (Utils.TimeChunk timeChunk : timeChunks) {

			System.out.println("Period: " + timeChunk.fromDate.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) + " - "
					+ timeChunk.toDate.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));

			// Run Logic for every time chunk
			QueryResult queryResult = Influx.query(FEMS, timeChunk.fromDate.minusSeconds(1),
					timeChunk.toDate.plusSeconds(1), settings.INFLUX_SOURCE_MEASUREMENT, converter.CHANNELS);
			Map<Long, Map<String, Object>> data;
			if (settings.INFLUX_SOURCE_MEASUREMENT == settings.INFLUX_TARGET_MEASUREMENT) {
				data = Influx.queryResultToList(queryResult);
			} else {
				// if source and target measurement are different: combine both
				QueryResult queryResult1 = Influx.query(FEMS, timeChunk.fromDate.minusSeconds(1),
						timeChunk.toDate.plusSeconds(1), settings.INFLUX_TARGET_MEASUREMENT, converter.CHANNELS);
				data = Influx.queryResultToList(queryResult, queryResult1);
			}

			BatchPoints batchPoints = Influx.createBatchPoints(FEMS, things, data, converter.FUNCTION);
			if (batchPoints != null) {
				try {
					Influx.write(batchPoints);
				} catch (InfluxDBIOException e) {
					System.out.println(" Error. Try again. " + e.getMessage());
					Influx.write(batchPoints);
				}
			}

		}

		System.out.println("Finished.");
	}

}
