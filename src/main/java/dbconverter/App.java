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

public class App {
	
	private final static Pattern cliArgPattern = Pattern.compile("^-([^=\\s]+)=(\\S*)$");

	private static int FEMS = 730;
//	public final static Types TYPE = Types.DESS;
	public final static Types TYPE = Types.OPENEMS_V1;

//	private static String FROM_DATE = "2018-10-29T00:00:00";
	private static String FROM_DATE = "";
//	private static String TO_DATE = "2018-10-30T00:00:00";
	private static String TO_DATE = "";
	
	private static boolean PRODUCTION = true;
	private static int RETRY_COUNT = 2;

	public static boolean OVERWRITE = false;

	public static void main(String[] args) throws Exception {
		
		parseArgs(args);
		
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

		List<Utils.TimeChunk> timeChunks = Utils.getTimeChunks(initialFromDate, initialToDate, Settings.CHUNK_DAYS, Settings.CHUNK_HOURS);
		
		List<Utils.TimeChunk> ignoredChunks = new ArrayList<>();
		int errors = 0;
		for (int i = 0; i < timeChunks.size(); i++) {
			Utils.TimeChunk timeChunk = timeChunks.get(i);
			try {
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
					Influx.write(batchPoints);
				}
			} catch(Exception e) {
				if (!PRODUCTION) {
					throw e;
				}
				System.out.println(e.getMessage());
				if (errors < RETRY_COUNT) {
					errors++;
					System.out.println("retrying with same period...");
					i--;
				} else {
					e.printStackTrace();
					errors = 0;
					ignoredChunks.add(timeChunk);
					System.out.println("too many errors with same period...continuing with next period");
				}
				continue;
			}
			errors = 0;
		}

		System.out.println("Finished.");
		if (ignoredChunks.size() != 0) {
			System.out.println("The following periods could not be processed due to some errors (view log for details):");
			for (Utils.TimeChunk c : ignoredChunks) {
				System.out.println(c);
			}
		}
	}
	
	private static void parseArgs(String[] args) throws Exception {
		for (String arg : args) {
			Matcher m = cliArgPattern.matcher(arg);
			if (m.matches()) {
				String v = m.group(2);
				switch (m.group(1)) {
				case "FEMS":
					FEMS = Integer.parseInt(v);
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
					Settings.CHUNK_DAYS = Integer.parseInt(v);
					break;
				case "CHUNK_HOURS":
					Settings.CHUNK_HOURS = Integer.parseInt(v);
					break;
				default:
					throw new Exception("illegal parameter: " + m.group(0));
				}
			} else {
				throw new Exception("illegal parameter format: " + arg);
			}
		}
	}

}
