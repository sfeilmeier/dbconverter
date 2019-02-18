package dbconverter;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import dbconverter.EdgeConfig.Component;
import dbconverter.influx.Influx.PointsFunction;

public class Converter {

	private final static String SUM_ESS_SOC = "_sum/EssSoc";
	private final static String SUM_ESS_ACTIVE_POWER = "_sum/EssActivePower";
	private final static String SUM_GRID_ACTIVE_POWER = "_sum/GridActivePower";
	private final static String SUM_PRODUCTION_ACTIVE_POWER = "_sum/ProductionActivePower";
	private final static String SUM_CONSUMPTION_ACTIVE_POWER = "_sum/ConsumptionActivePower";
	private final static String SUM_PRODUCTION_AC_ACTIVE_POWER = "_sum/ProductionAcActivePower";
	private final static String SUM_PRODUCTION_DC_ACTUAL_POWER = "_sum/ProductionDcActualPower";

	private final static String SOC = "%s/Soc";
	private final static String ACTIVE_POWER = "%s/ActivePower";
	private final static String ACTIVE_POWER_L1 = "%s/ActivePowerL1";
	private final static String ACTIVE_POWER_L2 = "%s/ActivePowerL2";
	private final static String ACTIVE_POWER_L3 = "%s/ActivePowerL3";
	private final static String ACTUAL_POWER = "%s/ActualPower";
	private final static String ACTUAL_ENERGY = "%s/ActualEnergy";
	private final static String ACTIVE_PRODUCTION_ENERGY = "%s/ActiveProductionEnergy";
	private final static String ACTIVE_CONSUMPTION_ENERGY = "%s/ActiveConsumptionEnergy";

	public final static String DESS_METER0_ACTIVE_POWER_L1 = "PCS1_Grid_Phase1_Active_Power";
	public final static String DESS_METER0_ACTIVE_POWER_L2 = "PCS2_Grid_Phase2_Active_Power";
	public final static String DESS_METER0_ACTIVE_POWER_L3 = "PCS3_Grid_Phase3_Active_Power";
	public final static String DESS_METER0_ACTIVE_PRODUCTION_ENERGY = "PCS_Summary_Grid_Accumulative_Electricity_Bought";
	public final static String DESS_METER0_ACTIVE_CONSUMPTION_ENERGY = "PCS_Summary_Grid_Accumulative_Electricity_Sold";
	public final static String DESS_SOC = "BSMU_Battery_Stack_Overall_SOC";
	public final static String DESS_CHARGER0_ACTUAL_POWER = "PV1_Charger1_Output_Power";
	public final static String DESS_CHARGER0_ACTUAL_ENERGY = "PV1_Charger1_Cumulative_Output";
	public final static String DESS_CHARGER1_ACTUAL_POWER = "PV2_Charger2_Output_Power";
	public final static String DESS_CHARGER1_ACTUAL_ENERGY = "PV2_Charger2_Cumulative_Output";
	public final static String DESS_METER1_ACTIVE_POWER_L1 = "PCS1_Phase1_PV_Inverter_Active_Power";
	public final static String DESS_METER1_ACTIVE_POWER_L2 = "PCS1_Phase1_PV_Inverter_Active_Power";
	public final static String DESS_METER1_ACTIVE_POWER_L3 = "PCS3_Phase3_PV_Inverter_Active_Power";
	public final static String DESS_CONSUMPTION_L1 = "PCS1_Phase1_Load_Active_Power";
	public final static String DESS_CONSUMPTION_L2 = "PCS2_Phase2_Load_Active_Power";
	public final static String DESS_CONSUMPTION_L3 = "PCS3_Phase3_Load_Active_Power";

	public final Set<String> CHANNELS;
	{
		Set<String> result = new HashSet<>();
		result.add(SUM_CONSUMPTION_ACTIVE_POWER);
		result.add(SUM_ESS_ACTIVE_POWER);
		result.add(SUM_ESS_SOC);
		result.add(SUM_GRID_ACTIVE_POWER);
		result.add(SUM_PRODUCTION_AC_ACTIVE_POWER);
		result.add(SUM_PRODUCTION_ACTIVE_POWER);
		result.add(SUM_PRODUCTION_DC_ACTUAL_POWER);

		for (String id : new String[] { "ess0", "ess1" }) {
			result.add(String.format(SOC, id));
			result.add(String.format(ACTIVE_POWER, id));
			result.add(String.format(ACTIVE_POWER_L1, id));
			result.add(String.format(ACTIVE_POWER_L2, id));
			result.add(String.format(ACTIVE_POWER_L3, id));
		}
		for (String id : new String[] { "meter0", "meter1", "meter2" }) {
			result.add(String.format(ACTIVE_POWER, id));
			result.add(String.format(ACTIVE_POWER_L1, id));
			result.add(String.format(ACTIVE_POWER_L2, id));
			result.add(String.format(ACTIVE_POWER_L3, id));
			result.add(String.format(ACTIVE_CONSUMPTION_ENERGY, id));
			result.add(String.format(ACTIVE_PRODUCTION_ENERGY, id));
		}
		for (String id : new String[] { "charger0", "charger1" }) {
			result.add(String.format(ACTUAL_POWER, id));
			result.add(String.format(ACTUAL_ENERGY, id));
		}

		switch (App.TYPE) {
		case DESS:
			result.add(DESS_METER0_ACTIVE_POWER_L1);
			result.add(DESS_METER0_ACTIVE_POWER_L2);
			result.add(DESS_METER0_ACTIVE_POWER_L3);
			result.add(DESS_METER0_ACTIVE_CONSUMPTION_ENERGY);
			result.add(DESS_METER0_ACTIVE_PRODUCTION_ENERGY);
			result.add(DESS_SOC);
			result.add(DESS_CHARGER0_ACTUAL_POWER);
			result.add(DESS_CHARGER0_ACTUAL_ENERGY);
			result.add(DESS_CHARGER1_ACTUAL_POWER);
			result.add(DESS_CHARGER1_ACTUAL_ENERGY);
			result.add(DESS_METER1_ACTIVE_POWER_L1);
			result.add(DESS_METER1_ACTIVE_POWER_L2);
			result.add(DESS_METER1_ACTIVE_POWER_L3);
			result.add(DESS_CONSUMPTION_L1);
			result.add(DESS_CONSUMPTION_L2);
			result.add(DESS_CONSUMPTION_L3);
			break;
		case OPENEMS_V1:
			for (String id : new String[] { "ess0", "ess1" }) {
				result.add(String.format(SOC, id));
				result.add(String.format(ACTIVE_POWER, id));
				result.add(String.format(ACTIVE_POWER_L1, id));
				result.add(String.format(ACTIVE_POWER_L2, id));
				result.add(String.format(ACTIVE_POWER_L3, id));
			}
			for (String id : new String[] { "meter0", "meter1", "meter2" }) {
				result.add(String.format(ACTIVE_POWER, id));
				result.add(String.format(ACTIVE_POWER_L1, id));
				result.add(String.format(ACTIVE_POWER_L2, id));
				result.add(String.format(ACTIVE_POWER_L3, id));
			}
			for (String id : new String[] { "charger0" }) {
				result.add(String.format(ACTUAL_POWER, id));
			}
			break;
		}
		CHANNELS = result;
	}

	public final PointsFunction FUNCTION = (things, input) -> {
		Map<String, Object> result = new HashMap<>();
		switch (App.TYPE) {
		case OPENEMS_V1:
			convertEssSoc(things.ess, result, input);
			convertEssPower(things.ess, result, input);
			convertGridPower(things.gridMeter, result, input);
			convertProductionAcPower(things.productionMeters, result, input);
			convertProductionDcPower(things.chargers, result, input);
			sumProductionPower(result, input);
			sumConsumptionPower(result, input);
			break;
		case DESS:
			convertDess(result, input);
			break;
		}
		return result;
	};

	private void copyValue(Map<String, Object> result, Map<String, Object> input, String targetChannel, Number sum) {
		if (sum == null) {
			return;
		}

		if (!App.OVERWRITE) {
			// do nothing if there is already a value
			Object existing = getValue(input, targetChannel);
			if (existing != null) {
				return;
			}
		}

		// copy value
		result.put(targetChannel, sum);
	}

	private static Integer add(Integer sum, Object value) {
		if (value == null) {
			return sum;
		}

		int intValue;
		if (value instanceof Double) {
			intValue = ((Double) value).intValue();
		} else if (value instanceof Integer) {
			intValue = (Integer) value;
		} else {
			throw new IllegalArgumentException("Unable to cast value " + value);
		}

		if (sum == null) {
			return intValue;
		} else {
			return sum + intValue;
		}
	}

	private static Long add(Long sum, Object value) {
		if (value == null) {
			return sum;
		}

		long longValue;
		if (value instanceof Double) {
			longValue = ((Double) value).longValue();
		} else if (value instanceof Long) {
			longValue = (Long) value;
		} else {
			throw new IllegalArgumentException("Unable to cast value " + value);
		}

		if (sum == null) {
			return longValue;
		} else {
			return sum + longValue;
		}
	}

	private Object getValue(Map<String, Object> values, String channel) {
		if (!CHANNELS.contains(channel)) {
			throw new IllegalArgumentException("Channel was not queried: " + channel);
		}
		return values.get(channel);
	}

	/**
	 * ess0/Soc + ess1/Soc + ... -> _sum/EssSoc
	 * 
	 * @param ess
	 * @param result
	 * @param input
	 */
	private void convertEssSoc(Map<String, Component> ess, Map<String, Object> result, Map<String, Object> input) {
		Integer sum = null;
		for (String id : ess.keySet()) {
			sum = add(sum, getValue(input, String.format(SOC, id)));
		}

		copyValue(result, input, SUM_ESS_SOC, sum);
	}

	/**
	 * ess0/ActivePower + ess1/ActivePower + ... -> _sum/EssActivePower
	 * 
	 * @param ess
	 * @param result
	 * @param input
	 * @throws Exception
	 */
	private void convertEssPower(Map<String, Component> ess, Map<String, Object> result, Map<String, Object> input)
			throws Exception {
		Integer sum = null;
		for (Entry<String, Component> entry : ess.entrySet()) {
			String factoryPid = entry.getValue().getFactoryPid();
			switch (factoryPid) {
			// ASYMMETRIC
			case "io.openems.impl.device.minireadonly.FeneconMiniEss":
			case "io.openems.impl.device.pro.FeneconProEss":
				sum = add(sum, getValue(input, String.format(ACTIVE_POWER_L1, entry.getKey())));
				sum = add(sum, getValue(input, String.format(ACTIVE_POWER_L2, entry.getKey())));
				sum = add(sum, getValue(input, String.format(ACTIVE_POWER_L3, entry.getKey())));
				break;

			// SYMMETRIC
			case "io.openems.impl.device.commercial.FeneconCommercialEss":
				sum = add(sum, getValue(input, String.format(ACTIVE_POWER, entry.getKey())));
				break;

			default:
				throw new Exception("Unknown ESS factory: " + factoryPid);
			}
		}

		copyValue(result, input, SUM_ESS_ACTIVE_POWER, sum);
	}

	/**
	 * meter0/ActivePower -> _sum/GridActivePower
	 * 
	 * @param gridMeter
	 * 
	 * @param result
	 * @param input
	 * @throws Exception
	 */
	private void convertGridPower(Entry<String, Component> gridMeter, Map<String, Object> result,
			Map<String, Object> input) throws Exception {
		Integer sum = getMeterPower(gridMeter, result, input);
		copyValue(result, input, SUM_GRID_ACTIVE_POWER, sum);
	}

	private Integer getMeterPower(Entry<String, Component> meter, Map<String, Object> result, Map<String, Object> input)
			throws Exception {
		Integer sum = null;
		String clazz = meter.getValue().getFactoryPid();
		switch (clazz) {
		// ASYMMETRIC
		case "io.openems.impl.device.pro.FeneconProPvMeter":
			sum = add(sum, getValue(input, String.format(ACTIVE_POWER_L1, meter.getKey())));
			sum = add(sum, getValue(input, String.format(ACTIVE_POWER_L2, meter.getKey())));
			sum = add(sum, getValue(input, String.format(ACTIVE_POWER_L3, meter.getKey())));
			break;

		// SYMMETRIC
		case "io.openems.impl.device.minireadonly.FeneconMiniProductionMeter":
		case "io.openems.impl.device.minireadonly.FeneconMiniGridMeter":
		case "io.openems.impl.device.socomec.SocomecMeter":
			sum = add(sum, getValue(input, String.format(ACTIVE_POWER, meter.getKey())));
			break;

		default:
			throw new Exception("Unknown Meter class: " + clazz);
		}

		return sum;
	}

	/**
	 * charger0/ActualPower -> _sum/ProductionDcActualPower
	 * 
	 * @param chargers
	 * @param result
	 * @param input
	 */
	private void convertProductionDcPower(Map<String, Component> chargers, Map<String, Object> result,
			Map<String, Object> input) {
		Integer sum = null;
		for (Entry<String, Component> entry : chargers.entrySet()) {
			sum = add(sum, getValue(input, String.format(ACTUAL_POWER, entry.getKey())));
		}
		copyValue(result, input, SUM_PRODUCTION_DC_ACTUAL_POWER, sum);
	}

	/**
	 * meter1/ActivePower -> _sum/ProductionAcActivePower
	 * 
	 * @param productionMeters
	 * @param result
	 * @param input
	 * @throws Exception
	 */
	private void convertProductionAcPower(Map<String, Component> productionMeters, Map<String, Object> result,
			Map<String, Object> input) throws Exception {
		Integer sum = null;
		for (Entry<String, Component> entry : productionMeters.entrySet()) {
			sum = add(sum, getMeterPower(entry, result, input));
		}
		copyValue(result, input, SUM_PRODUCTION_AC_ACTIVE_POWER, sum);
	}

	/**
	 * _sum/ProductionDcActualPower + _sum/ProductionAcActivePower ->
	 * _sum/ProductionActivePower
	 * 
	 * @param result
	 * @param input
	 * @param input
	 */
	private void sumProductionPower(Map<String, Object> result, Map<String, Object> input) {
		Integer sum = null;
		sum = add(sum, getValue(result, SUM_PRODUCTION_AC_ACTIVE_POWER));
		sum = add(sum, getValue(result, SUM_PRODUCTION_DC_ACTUAL_POWER));
		copyValue(result, input, SUM_PRODUCTION_ACTIVE_POWER, sum);
	}

	/**
	 * _sum/EssActivePower + _sum/GridActivePower + _sum/ProductionAcActivePower ->
	 * _sum/ConsumptionActivePower
	 * 
	 * @param result
	 * @param input
	 * @param input
	 */
	private void sumConsumptionPower(Map<String, Object> result, Map<String, Object> input) {
		Integer sum = null;
		sum = add(sum, getValue(result, SUM_ESS_ACTIVE_POWER));
		sum = add(sum, getValue(result, SUM_GRID_ACTIVE_POWER));
		sum = add(sum, getValue(result, SUM_PRODUCTION_AC_ACTIVE_POWER));
		copyValue(result, input, SUM_CONSUMPTION_ACTIVE_POWER, sum);
	}

	/**
	 * DESS -> ess0, meter0, _sum/...
	 * 
	 * @param gridMeter
	 * @param result
	 * @param input
	 * @throws Exception
	 */
	private void convertDess(Map<String, Object> result, Map<String, Object> input) throws Exception {
		// Grid
		Integer gridPower = null;
		{
			Integer meter0ActivePowerL1 = null;
			meter0ActivePowerL1 = add(meter0ActivePowerL1, getValue(input, DESS_METER0_ACTIVE_POWER_L1));
			copyValue(result, input, String.format(ACTIVE_POWER_L1, "meter0"), meter0ActivePowerL1);
			if (meter0ActivePowerL1 != null) {
				meter0ActivePowerL1 *= -1;
			}

			Integer meter0ActivePowerL2 = null;
			meter0ActivePowerL2 = add(meter0ActivePowerL2, getValue(input, DESS_METER0_ACTIVE_POWER_L2));
			copyValue(result, input, String.format(ACTIVE_POWER_L2, "meter0"), meter0ActivePowerL2);
			if (meter0ActivePowerL2 != null) {
				meter0ActivePowerL2 *= -1;
			}

			Integer meter0ActivePowerL3 = null;
			meter0ActivePowerL3 = add(meter0ActivePowerL3, getValue(input, DESS_METER1_ACTIVE_POWER_L3));
			copyValue(result, input, String.format(ACTIVE_POWER_L3, "meter0"), meter0ActivePowerL3);
			if (meter0ActivePowerL3 != null) {
				meter0ActivePowerL3 *= -1;
			}

			gridPower = add(gridPower, meter0ActivePowerL1);
			gridPower = add(gridPower, meter0ActivePowerL2);
			gridPower = add(gridPower, meter0ActivePowerL3);
			copyValue(result, input, String.format(ACTIVE_POWER, "meter0"), gridPower);
			copyValue(result, input, SUM_GRID_ACTIVE_POWER, gridPower);

			Integer meter0ActiveProductionEnergy = null;
			meter0ActiveProductionEnergy = add(meter0ActiveProductionEnergy,
					getValue(input, DESS_METER0_ACTIVE_PRODUCTION_ENERGY));
			if (meter0ActiveProductionEnergy != null) {
				meter0ActiveProductionEnergy *= 100;
			}
			copyValue(result, input, String.format(ACTIVE_PRODUCTION_ENERGY, "meter0"), meter0ActiveProductionEnergy);

			Integer meter0ActiveConsumptionEnergy = null;
			meter0ActiveConsumptionEnergy = add(meter0ActiveConsumptionEnergy,
					getValue(input, DESS_METER0_ACTIVE_CONSUMPTION_ENERGY));
			if (meter0ActiveConsumptionEnergy != null) {
				meter0ActiveConsumptionEnergy *= 100;
			}
			copyValue(result, input, String.format(ACTIVE_CONSUMPTION_ENERGY, "meter0"), meter0ActiveConsumptionEnergy);
		}

		// SoC
		Integer soc = null;
		soc = add(soc, getValue(input, DESS_SOC));
		copyValue(result, input, SUM_ESS_SOC, soc);

		// Production DC
		Integer productionDcPower = null;
		{
			Integer charger0ActualPower = null;
			charger0ActualPower = add(charger0ActualPower, getValue(input, DESS_CHARGER0_ACTUAL_POWER));
			copyValue(result, input, String.format(ACTUAL_POWER, "charger0"), charger0ActualPower);

			Integer charger1ActualPower = null;
			charger1ActualPower = add(charger1ActualPower, getValue(input, DESS_CHARGER1_ACTUAL_POWER));
			copyValue(result, input, String.format(ACTUAL_POWER, "charger1"), charger1ActualPower);

			productionDcPower = add(productionDcPower, charger0ActualPower);
			productionDcPower = add(productionDcPower, charger1ActualPower);
			copyValue(result, input, SUM_PRODUCTION_DC_ACTUAL_POWER, productionDcPower);

			Long charger0ActualEnergy = null;
			charger0ActualEnergy = add(charger0ActualEnergy, getValue(input, DESS_CHARGER0_ACTUAL_ENERGY));
			if (charger0ActualEnergy != null) {
				charger0ActualEnergy = (charger0ActualEnergy + (long) Math.pow(2, 32)) * 100;
			}
			copyValue(result, input, String.format(ACTUAL_ENERGY, "charger0"), charger0ActualEnergy);

			Long charger1ActualEnergy = null;
			charger1ActualEnergy = add(charger1ActualEnergy, getValue(input, DESS_CHARGER1_ACTUAL_ENERGY));
			if (charger1ActualEnergy != null) {
				charger1ActualEnergy = (charger1ActualEnergy + (long) Math.pow(2, 32)) * 100;
			}
			copyValue(result, input, String.format(ACTUAL_ENERGY, "charger1"), charger1ActualEnergy);
		}

		// Production AC
		Integer productionAcPower = null;
		{
			Integer meter1ActivePowerL1 = null;
			meter1ActivePowerL1 = add(meter1ActivePowerL1, getValue(input, DESS_METER1_ACTIVE_POWER_L1));
			copyValue(result, input, String.format(ACTIVE_POWER_L1, "meter1"), meter1ActivePowerL1);

			Integer meter1ActivePowerL2 = null;
			meter1ActivePowerL2 = add(meter1ActivePowerL2, getValue(input, DESS_METER1_ACTIVE_POWER_L2));
			copyValue(result, input, String.format(ACTIVE_POWER_L2, "meter1"), meter1ActivePowerL2);

			Integer meter1ActivePowerL3 = null;
			meter1ActivePowerL3 = add(meter1ActivePowerL3, getValue(input, DESS_METER1_ACTIVE_POWER_L3));
			copyValue(result, input, String.format(ACTIVE_POWER_L3, "meter1"), meter1ActivePowerL3);

			productionAcPower = add(productionAcPower, meter1ActivePowerL1);
			productionAcPower = add(productionAcPower, meter1ActivePowerL2);
			productionAcPower = add(productionAcPower, meter1ActivePowerL3);
			copyValue(result, input, String.format(ACTIVE_POWER, "meter1"), gridPower);
			copyValue(result, input, SUM_PRODUCTION_AC_ACTIVE_POWER, productionAcPower);
		}

		// Production Total
		Integer productionPower = null;
		productionPower = add(productionPower, productionDcPower);
		productionPower = add(productionPower, productionAcPower);
		copyValue(result, input, SUM_PRODUCTION_ACTIVE_POWER, productionPower);

		// Consumption
		Integer consumptionPower = null;
		{
			consumptionPower = add(consumptionPower, getValue(input, DESS_CONSUMPTION_L1));
			consumptionPower = add(consumptionPower, getValue(input, DESS_CONSUMPTION_L2));
			consumptionPower = add(consumptionPower, getValue(input, DESS_CONSUMPTION_L3));
			copyValue(result, input, SUM_CONSUMPTION_ACTIVE_POWER, consumptionPower);
		}

		// Charge/Discharge
		{
			if (consumptionPower != null && gridPower != null && productionPower != null) {
				Integer charge = consumptionPower - gridPower - productionPower;
				copyValue(result, input, String.format(ACTIVE_POWER, "ess0"), charge);
				copyValue(result, input, SUM_ESS_ACTIVE_POWER, charge);
			}
		}
	}

}
