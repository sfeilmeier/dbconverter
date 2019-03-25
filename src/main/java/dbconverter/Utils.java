package dbconverter;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.influxdb.dto.QueryResult;

import dbconverter.EdgeConfig.Component;
import dbconverter.influx.Influx;

public class Utils {

	/**
	 * Gets the From-Date from the given string or - if empty - the first ever
	 * timestamp.
	 * 
	 * @param femsId
	 * @param date
	 * @return
	 */
	protected static ZonedDateTime getFromDate(int femsId, String date) {
		if (date.isEmpty()) {
			return getFirstTimestamp(femsId);
		} else {
			return LocalDateTime.parse(date).atZone(ZoneId.systemDefault());
		}
	}

	/**
	 * Gets the To-Date from the given string or - if empty - the current timestamp.
	 * 
	 * @param femsId
	 * @param date
	 * @return
	 */
	protected static ZonedDateTime getToDate(String date) {
		if (date.isEmpty()) {
			return ZonedDateTime.now();
		} else {
			return LocalDateTime.parse(date).atZone(ZoneId.systemDefault());
		}
	}

	public static class TimeChunk {
		ZonedDateTime fromDate;
		ZonedDateTime toDate;
	}

	/**
	 * Splits the period between fromDate and toDate in chunks with length chunkDays
	 * 
	 * @param initialFromDate
	 * @param initialToDate
	 * @param chunkDays
	 * @return
	 */
	protected static List<TimeChunk> getTimeChunks(ZonedDateTime initialFromDate, ZonedDateTime initialToDate,
			int chunkDays) {
		List<TimeChunk> result = new ArrayList<>();

		ZonedDateTime fromDate = initialFromDate;
		ZonedDateTime toDate = null;
		while (toDate == null || toDate.isBefore(initialToDate)) {
			// get next toDate
			toDate = fromDate.plusDays(chunkDays);
			if (toDate.isAfter(initialToDate)) {
				toDate = initialToDate;
			}

			// create chunk
			TimeChunk chunk = new TimeChunk();
			chunk.fromDate = fromDate;
			chunk.toDate = toDate;
			result.add(chunk);

			// get next fromDate
			fromDate = toDate;
		}

		return result;
	}

	public static class Things {
		Map<String, Component> ess = new HashMap<>();
		Map.Entry<String, Component> gridMeter = null;
		Map<String, Component> productionMeters = new HashMap<>();
		Map<String, Component> chargers = new HashMap<>();

		protected void assertValues() throws Exception {
			if (ess.isEmpty()) {
				throw new Exception("Ess is not set: " + ess);
			}
			if (gridMeter == null) {
				throw new Exception("GridMeter is not set: " + gridMeter);
			}
		}
	}

	public static Things getThings(EdgeConfig config) throws Exception {
		Things result = new Things();

		TreeMap<String, Component> components = config.getComponents();
		for (Entry<String, Component> entry : components.entrySet()) {
			Component component = entry.getValue();
			String clazz = component.getFactoryId();
			String id = entry.getKey();
			if (!clazz.startsWith("io.openems.impl.device.") || id.startsWith("_")) {
				continue;
			}
			switch (id) {
			case "system0":
			case "evcs0":
			case "output0":
				// ignore
				break;

			default:
				if (id.startsWith("meter")) {
					if (id.equals("meter0")) {
						result.gridMeter = new AbstractMap.SimpleEntry<String, Component>(id, component);
					} else {
						result.productionMeters.put(id, component);
					}

				} else if (id.startsWith("ess")) {
					result.ess.put(id, component);

				} else if (id.startsWith("charger")) {
					result.chargers.put(id, component);

				} else {
					throw new Exception("Undefined component: " + component);
				}
			}
		}
		result.assertValues();
		return result;
	}

	public static ZonedDateTime getFirstTimestamp(int femsId) {
		Settings settings = new Settings();

		String socField;
		switch (App.TYPE) {
		case DESS:
			socField = Converter.DESS_SOC;
			break;
		case OPENEMS_V1:
		default:
			socField = "ess0/Soc";
			break;
		}
		QueryResult result = Influx.query("SELECT \"" + socField + "\", time FROM " + settings.INFLUX_SOURCE_MEASUREMENT
				+ " WHERE fems = '" + femsId + "' AND time > '2010-01-01' LIMIT 1");
		long timestamp = ((Double) result.getResults().get(0).getSeries().get(0).getValues().get(0).get(0)).longValue();
		Instant instant = Instant.ofEpochMilli(timestamp);
		return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC);
	}

}
