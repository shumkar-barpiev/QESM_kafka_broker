package com.myexam.qesm.analysis;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.List;
import java.util.Map;

import org.oristool.models.ValidationMessageCollector;
import org.oristool.models.stpn.SteadyStateSolution;
import org.oristool.models.stpn.steady.RegSteadyState;
import org.oristool.petrinet.Marking;
import org.oristool.petrinet.PetriNet;
import org.oristool.petrinet.Place;
import org.oristool.petrinet.Transition;

import com.myexam.qesm.kafka.KafkaBrokerModel;
import com.myexam.qesm.kafka.KafkaBrokerModel.Parameters;

/**
 * Runs Sirio regenerative steady-state analysis for the bounded B=3 Kafka model.
 */
public final class RegenerativeSteadyState {
	private static final MathContext MC = MathContext.DECIMAL64;
	private static final List<BigDecimal> ARRIVAL_RATES = decimals("0.25", "0.5", "1", "2");
	private static final List<BigDecimal> TIMEOUTS = decimals("1", "3", "5");

	private RegenerativeSteadyState() {
	}

	public static void main(String[] args) {
		System.out.println(SteadyStateResult.csvHeader());

		for (BigDecimal arrivalRate : ARRIVAL_RATES) {
			for (BigDecimal timeout : TIMEOUTS) {
				Parameters parameters = new Parameters(
						arrivalRate,
						timeout,
						BigDecimal.ONE,
						BigDecimal.ONE,
						BigDecimal.ONE);

				System.out.println(run(parameters).toCsv());
			}
		}
	}

	public static SteadyStateResult run(Parameters parameters) {
		PetriNet net = new PetriNet();
		Marking initialMarking = new Marking();
		KafkaBrokerModel.build(net, initialMarking, parameters);

		RegSteadyState analysis = RegSteadyState.builder().build();
		ValidationMessageCollector validation = new ValidationMessageCollector();
		if (!analysis.canAnalyze(net, validation)) {
			throw new IllegalArgumentException("Sirio cannot analyze this model: " + validation.getMessages());
		}

		SteadyStateSolution<Marking> solution = analysis.compute(net, initialMarking);
		return calculateMetrics(net, solution.getSteadyState(), parameters);
	}

	private static SteadyStateResult calculateMetrics(
			PetriNet net,
			Map<Marking, BigDecimal> probabilities,
			Parameters parameters) {

		Place buffer = net.getPlace("P_Buffer");
		Place batchService = net.getPlace("P_BatchService");
		Transition arrival = net.getTransition("ArrivalAccepted");
		Transition send1 = net.getTransition("T_Send1");
		Transition send2 = net.getTransition("T_Send2");
		Transition send3 = net.getTransition("T_Send3");

		BigDecimal probabilitySum = BigDecimal.ZERO;
		BigDecimal averageBuffer = BigDecimal.ZERO;
		BigDecimal averageInService = BigDecimal.ZERO;
		BigDecimal arrivalEnabledProbability = BigDecimal.ZERO;
		BigDecimal send1EnabledProbability = BigDecimal.ZERO;
		BigDecimal send2EnabledProbability = BigDecimal.ZERO;
		BigDecimal send3EnabledProbability = BigDecimal.ZERO;

		for (Map.Entry<Marking, BigDecimal> state : probabilities.entrySet()) {
			Marking marking = state.getKey();
			BigDecimal probability = state.getValue();
			probabilitySum = probabilitySum.add(probability, MC);
			averageBuffer = averageBuffer.add(
					probability.multiply(BigDecimal.valueOf(marking.getTokens(buffer)), MC), MC);
			averageInService = averageInService.add(
					probability.multiply(BigDecimal.valueOf(marking.getTokens(batchService)), MC), MC);

			if (net.isEnabled(arrival, marking)) {
				arrivalEnabledProbability = arrivalEnabledProbability.add(probability, MC);
			}
			if (net.isEnabled(send1, marking)) {
				send1EnabledProbability = send1EnabledProbability.add(probability, MC);
			}
			if (net.isEnabled(send2, marking)) {
				send2EnabledProbability = send2EnabledProbability.add(probability, MC);
			}
			if (net.isEnabled(send3, marking)) {
				send3EnabledProbability = send3EnabledProbability.add(probability, MC);
			}
		}

		BigDecimal effectiveArrivalRate = parameters.arrivalRate()
				.multiply(arrivalEnabledProbability, MC);
		BigDecimal batches1 = parameters.send1Rate().multiply(send1EnabledProbability, MC);
		BigDecimal batches2 = parameters.send2Rate().multiply(send2EnabledProbability, MC);
		BigDecimal batches3 = parameters.send3Rate().multiply(send3EnabledProbability, MC);
		BigDecimal batchThroughput = batches1.add(batches2, MC).add(batches3, MC);
		BigDecimal messageThroughput = batches1
				.add(batches2.multiply(BigDecimal.valueOf(2), MC), MC)
				.add(batches3.multiply(BigDecimal.valueOf(3), MC), MC);
		BigDecimal averageBatchSize = divide(messageThroughput, batchThroughput);
		BigDecimal queueWaitingTime = divide(averageBuffer, effectiveArrivalRate);
		BigDecimal totalSystemTime = divide(
				averageBuffer.add(averageInService, MC), effectiveArrivalRate);

		return new SteadyStateResult(
				parameters.arrivalRate(),
				parameters.timeout(),
				probabilities.size(),
				probabilitySum,
				effectiveArrivalRate,
				averageBuffer,
				batchThroughput,
				messageThroughput,
				averageBatchSize,
				queueWaitingTime,
				totalSystemTime);
	}

	private static BigDecimal divide(BigDecimal numerator, BigDecimal denominator) {
		return denominator.signum() == 0 ? BigDecimal.ZERO : numerator.divide(denominator, MC);
	}

	private static List<BigDecimal> decimals(String... values) {
		return java.util.Arrays.stream(values).map(BigDecimal::new).toList();
	}

	public record SteadyStateResult(
			BigDecimal arrivalRate,
			BigDecimal timeout,
			int states,
			BigDecimal probabilitySum,
			BigDecimal effectiveArrivalRate,
			BigDecimal averageBuffer,
			BigDecimal batchThroughput,
			BigDecimal messageThroughput,
			BigDecimal averageBatchSize,
			BigDecimal queueWaitingTime,
			BigDecimal totalSystemTime) {

		public static String csvHeader() {
			return "lambda,timeout,B,states,probability_sum,effective_arrival_rate,"
					+ "average_buffer,batch_throughput,message_throughput,average_batch_size,"
					+ "queue_waiting_time,total_system_time";
		}

		public String toCsv() {
			return String.join(",",
					arrivalRate.toPlainString(),
					timeout.toPlainString(),
					Integer.toString(KafkaBrokerModel.FULL_BATCH_SIZE),
					Integer.toString(states),
					probabilitySum.toPlainString(),
					effectiveArrivalRate.toPlainString(),
					averageBuffer.toPlainString(),
					batchThroughput.toPlainString(),
					messageThroughput.toPlainString(),
					averageBatchSize.toPlainString(),
					queueWaitingTime.toPlainString(),
					totalSystemTime.toPlainString());
		}
	}
}
