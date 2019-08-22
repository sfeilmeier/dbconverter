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
	 * @param chunkHours
	 * @return
	 */
	protected static List<TimeChunk> getTimeChunks(ZonedDateTime initialFromDate, ZonedDateTime initialToDate,
			int chunkDays, int chunkHours) {
		List<TimeChunk> result = new ArrayList<>();

		ZonedDateTime fromDate = initialFromDate;
		ZonedDateTime toDate = null;
		while (toDate == null || toDate.isBefore(initialToDate)) {
			// get next toDate
			toDate = fromDate.plusDays(chunkDays);
			toDate = toDate.plusHours(chunkHours);
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
		Map<String, Component> evcs = new HashMap<>();

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
			String id = entry.getKey();
			if (id.startsWith("_")) {
				continue;
			}
			String idWithoutNumber = id.replaceAll("\\d*$", "");
			switch (idWithoutNumber) {
			case "system":
			case "output":
			case "io":
			case "ctrlApiRest":
			case "ctrlApiWebsocket":
			case "ctrlApiModbusTcp":
			case "ctrlBackend":
			case "ctrlBalancing":
			case "ctrlDebugLog":
			case "ctrlEvcs":
			case "ctrlLimitActivePower":
			case "ctrlLimitTotalDischarge":
			case "ctrlChannelThreshold":
			case "ctrlCommercial40SurplusFeedIn":
			case "ctrlEssAcIsland":
			case "influx":
			case "modbus":
			case "scheduler":
				// ignore
				break;

			case "meter":
				if (id.equals("meter0")) {
					result.gridMeter = new AbstractMap.SimpleEntry<String, Component>(id, component);
				} else {
					result.productionMeters.put(id, component);
				}
				break;

			case "ess":
				result.ess.put(id, component);
				break;

			case "charger":
				result.chargers.put(id, component);
				break;

			case "evcs":
				result.evcs.put(id, component);
				break;

			default:
				throw new Exception("Undefined component: " + id);
			}
		}
		result.assertValues();
		return result;
	}

	public static ZonedDateTime getFirstTimestamp(int femsId) {
		Settings settings = new Settings();

		String socField;
		switch (DbConverterApp.TYPE) {
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
