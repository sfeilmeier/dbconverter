package dbconverter;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.QueryResult;

import dbconverter.Settings.Types;
import dbconverter.Utils.Things;
import dbconverter.influx.Influx;
import dbconverter.odoo.Odoo;

public class DbConverterApp {

	private final static Pattern cliArgPattern = Pattern.compile("^-([^=\\s]+)=(\\S*)$");

	private static int[] FEMS = new int[] { 791, 892, 1041, 1081 };
//	public final static Types TYPE = Types.DESS;
	public final static Types TYPE = Types.OPENEMS_V1;

//	private static String FROM_DATE = "2019-03-25T00:00:00";
	private static String FROM_DATE = "";
//	private static String TO_DATE = "2018-10-30T00:00:00";
	private static String TO_DATE = "";

	private static int CHUNK_DAYS = 3;
	private static int CHUNK_HOURS = 0;

	private static boolean PRODUCTION = true;
	private static int RETRY_COUNT = 2;

	public static boolean OVERWRITE = false;

	public static void main(String[] args) throws Exception {

		parseArgs(args);

		Converter converter = new Converter();
		Settings settings = new Settings();

		for (int femsId : FEMS) {
			System.out.println(femsId + ": Starting");

			// Get configuration
			Things things = null;
			try {
				if (TYPE == Types.OPENEMS_V1 || TYPE == Types.OPENHAB_IO) {
					EdgeConfig config = Odoo.getConfig(femsId);
					things = Utils.getThings(config);
				}
			} catch (ClassCastException e) {
				// this happens when there is no config
			}

			// Get start/end date
			ZonedDateTime initialFromDate;
			try {
				initialFromDate = Utils.getFromDate(femsId, FROM_DATE);
			} catch (NullPointerException e) {
				System.out.println(femsId + ": Unable to read 'From-Timestamp'");
				System.out.println();
				continue;
			}
			ZonedDateTime initialToDate = Utils.getToDate(TO_DATE);

			List<Utils.TimeChunk> timeChunks = Utils.getTimeChunks(initialFromDate, initialToDate, CHUNK_DAYS,
					CHUNK_HOURS);

			List<Utils.TimeChunk> ignoredChunks = new ArrayList<>();
			int errors = 0;
			for (int i = 0; i < timeChunks.size(); i++) {
				Utils.TimeChunk timeChunk = timeChunks.get(i);
				try {
					System.out.println(
							femsId + ": Period: " + timeChunk.fromDate.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
									+ " - " + timeChunk.toDate.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));

					// Run Logic for every time chunk
					QueryResult queryResult = Influx.query(femsId, timeChunk.fromDate.minusSeconds(1),
							timeChunk.toDate.plusSeconds(1), settings.INFLUX_SOURCE_MEASUREMENT, converter.CHANNELS);
					Map<Long, Map<String, Object>> data;
					if (settings.INFLUX_SOURCE_MEASUREMENT == settings.INFLUX_TARGET_MEASUREMENT) {
						data = Influx.queryResultToList(queryResult);
					} else {
						// if source and target measurement are different: combine both
						QueryResult queryResult1 = Influx.query(femsId, timeChunk.fromDate.minusSeconds(1),
								timeChunk.toDate.plusSeconds(1), settings.INFLUX_TARGET_MEASUREMENT,
								converter.CHANNELS);
						data = Influx.queryResultToList(queryResult, queryResult1);
					}

					BatchPoints batchPoints = Influx.createBatchPoints(femsId, things, data, converter.FUNCTION);
					if (batchPoints != null) {
						Influx.write(batchPoints);
					}
				} catch (Exception e) {
					if (!PRODUCTION) {
						throw e;
					}
					System.out.println(e.getMessage());
					if (errors < RETRY_COUNT) {
						errors++;
						System.out.println(femsId + ": retrying with same period...");
						i--;
					} else {
						e.printStackTrace();
						errors = 0;
						ignoredChunks.add(timeChunk);
						System.out.println(femsId + ": too many errors with same period...continuing with next period");
					}
					continue;
				}
				errors = 0;
			}

			System.out.println(femsId + ": Finished.");
			if (ignoredChunks.size() != 0) {
				System.out.println(femsId
						+ ": The following periods could not be processed due to some errors (view log for details):");
				for (Utils.TimeChunk c : ignoredChunks) {
					System.out.println(c);
				}
			}
		}

	}

	private static void parseArgs(String[] args) throws Exception {
		List<Integer> fems = new ArrayList<>();
		for (String arg : args) {
			Matcher m = cliArgPattern.matcher(arg);
			if (m.matches()) {
				String v = m.group(2);
				switch (m.group(1)) {
				case "FEMS":
					fems.add(Integer.parseInt(v));
					break;
				case "FROM_DATE":
					FROM_DATE = v;
					break;
				case "TO_DATE":
					TO_DATE = v;
					break;
				case "PRODUCTION":
					PRODUCTION = Boolean.parseBoolean(v);
					break;
				case "RETRY_COUNT":
					RETRY_COUNT = Integer.parseInt(v);
					break;
				case "OVERWRITE":
					OVERWRITE = Boolean.parseBoolean(v);
					break;
				case "INFLUX_URL":
					Settings.INFLUX_URL = v;
					break;
				case "INFLUX_USER":
					Settings.INFLUX_USER = v;
					break;
				case "INFLUX_PASSWORD":
					Settings.INFLUX_PASSWORD = v;
					break;
				case "CHUNK_DAYS":
					CHUNK_DAYS = Integer.parseInt(v);
					break;
				case "CHUNK_HOURS":
					CHUNK_HOURS = Integer.parseInt(v);
					break;
				default:
					throw new Exception("illegal parameter: " + m.group(0));
				}
			} else {
				throw new Exception("illegal parameter format: " + arg);
			}
		}

		if (!fems.isEmpty()) {
			FEMS = fems.stream().mapToInt(i -> i).toArray();
		}
	}

}
