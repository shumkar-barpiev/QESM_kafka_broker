package com.myexam.qesm.experiments.transientanalysis;

import java.math.BigDecimal;
import java.util.List;

import org.oristool.models.ValidationMessageCollector;
import org.oristool.models.stpn.TransientSolution;
import org.oristool.models.stpn.trans.RegTransient;
import org.oristool.models.stpn.trees.DeterministicEnablingState;
import org.oristool.petrinet.Marking;
import org.oristool.petrinet.PetriNet;
import org.oristool.petrinet.Place;
import org.oristool.petrinet.Transition;

import com.myexam.qesm.kafka.KafkaBrokerModel;
import com.myexam.qesm.kafka.KafkaBrokerModel.Parameters;

/**
 * Examines startup behaviour from an empty broker before steady state is reached.
 */
public final class TransientExperiment {
	private static final BigDecimal TIME_BOUND = new BigDecimal("15");
	private static final BigDecimal TIME_STEP = new BigDecimal("0.5");
	private static final BigDecimal TIMEOUT = new BigDecimal("3");
	private static final List<BigDecimal> ARRIVAL_RATES = decimals("0.25", "1", "2");

	private TransientExperiment() {
	}

	public static void main(String[] args) {
		System.out.println(Sample.csvHeader());
		for (BigDecimal arrivalRate : ARRIVAL_RATES) {
			Parameters parameters = new Parameters(
					arrivalRate,
					TIMEOUT,
					BigDecimal.ONE,
					BigDecimal.ONE,
					BigDecimal.ONE);

			for (Sample sample : run(parameters, TIME_BOUND, TIME_STEP)) {
				System.out.println(sample.toCsv());
			}
		}
	}

	public static List<Sample> run(Parameters parameters, BigDecimal timeBound, BigDecimal timeStep) {
		validateTimeGrid(timeBound, timeStep);

		PetriNet net = new PetriNet();
		Marking initialMarking = new Marking();
		KafkaBrokerModel.build(net, initialMarking, parameters);

		RegTransient analysis = RegTransient.builder()
				.timeBound(timeBound)
				.timeStep(timeStep)
				.build();
		ValidationMessageCollector validation = new ValidationMessageCollector();
		if (!analysis.canAnalyze(net, validation)) {
			throw new IllegalArgumentException("Sirio cannot analyze this model: " + validation.getMessages());
		}

		TransientSolution<DeterministicEnablingState, Marking> solution =
				analysis.compute(net, initialMarking);
		return calculateSamples(net, solution, parameters);
	}

	private static List<Sample> calculateSamples(
			PetriNet net,
			TransientSolution<DeterministicEnablingState, Marking> solution,
			Parameters parameters) {

		Place buffer = net.getPlace("P_Buffer");
		Place batchService = net.getPlace("P_BatchService");
		Place brokerBusy = net.getPlace("P_BrokerBusy");
		Place timerActive = net.getPlace("P_TimerActive");
		Transition arrival = net.getTransition("ArrivalAccepted");
		Transition send1 = net.getTransition("T_Send1");
		Transition send2 = net.getTransition("T_Send2");
		Transition send3 = net.getTransition("T_Send3");

		int initialIndex = solution.getRegenerations().indexOf(solution.getInitialRegeneration());
		if (initialIndex < 0) {
			throw new IllegalStateException("Initial regeneration is missing from the transient solution");
		}

		List<Marking> markings = solution.getColumnStates();
		double[][][] values = solution.getSolution();
		double step = solution.getStep().doubleValue();
		java.util.ArrayList<Sample> samples = new java.util.ArrayList<>(solution.getSamplesNumber());
		double cumulativeBatches = 0.0;
		double cumulativeMessages = 0.0;
		double previousBatchRate = 0.0;
		double previousMessageRate = 0.0;

		for (int timeIndex = 0; timeIndex < solution.getSamplesNumber(); timeIndex++) {
			double probabilitySum = 0.0;
			double[] bufferProbabilities = new double[KafkaBrokerModel.FULL_BATCH_SIZE + 1];
			double busyProbability = 0.0;
			double timerActiveProbability = 0.0;
			double averageBuffer = 0.0;
			double averageInService = 0.0;
			double arrivalEnabledProbability = 0.0;
			double send1EnabledProbability = 0.0;
			double send2EnabledProbability = 0.0;
			double send3EnabledProbability = 0.0;

			for (int stateIndex = 0; stateIndex < markings.size(); stateIndex++) {
				Marking marking = markings.get(stateIndex);
				double probability = values[timeIndex][initialIndex][stateIndex];
				int bufferedMessages = marking.getTokens(buffer);
				probabilitySum += probability;
				if (bufferedMessages >= 0 && bufferedMessages < bufferProbabilities.length) {
					bufferProbabilities[bufferedMessages] += probability;
				}
				averageBuffer += bufferedMessages * probability;
				averageInService += marking.getTokens(batchService) * probability;
				if (marking.getTokens(brokerBusy) > 0) {
					busyProbability += probability;
				}
				if (marking.getTokens(timerActive) > 0) {
					timerActiveProbability += probability;
				}
				if (net.isEnabled(arrival, marking)) {
					arrivalEnabledProbability += probability;
				}
				if (net.isEnabled(send1, marking)) {
					send1EnabledProbability += probability;
				}
				if (net.isEnabled(send2, marking)) {
					send2EnabledProbability += probability;
				}
				if (net.isEnabled(send3, marking)) {
					send3EnabledProbability += probability;
				}
			}

			double batch1Rate = parameters.send1Rate().doubleValue() * send1EnabledProbability;
			double batch2Rate = parameters.send2Rate().doubleValue() * send2EnabledProbability;
			double batch3Rate = parameters.send3Rate().doubleValue() * send3EnabledProbability;
			double batchRate = batch1Rate + batch2Rate + batch3Rate;
			double messageRate = batch1Rate + 2.0 * batch2Rate + 3.0 * batch3Rate;
			if (timeIndex > 0) {
				cumulativeBatches += 0.5 * step * (previousBatchRate + batchRate);
				cumulativeMessages += 0.5 * step * (previousMessageRate + messageRate);
			}

			samples.add(new Sample(
					parameters.arrivalRate(),
					parameters.timeout(),
					solution.getStep().multiply(BigDecimal.valueOf(timeIndex)),
					probabilitySum,
					bufferProbabilities[0],
					bufferProbabilities[1],
					bufferProbabilities[2],
					bufferProbabilities[3],
					busyProbability,
					timerActiveProbability,
					averageBuffer,
					averageInService,
					parameters.arrivalRate().doubleValue() * arrivalEnabledProbability,
					batchRate,
					messageRate,
					cumulativeBatches,
					cumulativeMessages));

			previousBatchRate = batchRate;
			previousMessageRate = messageRate;
		}

		return List.copyOf(samples);
	}

	private static void validateTimeGrid(BigDecimal timeBound, BigDecimal timeStep) {
		if (timeBound == null || timeBound.signum() <= 0) {
			throw new IllegalArgumentException("timeBound must be greater than zero");
		}
		if (timeStep == null || timeStep.signum() <= 0) {
			throw new IllegalArgumentException("timeStep must be greater than zero");
		}
		if (timeBound.remainder(timeStep).signum() != 0) {
			throw new IllegalArgumentException("timeBound must be an exact multiple of timeStep");
		}
	}

	private static List<BigDecimal> decimals(String... values) {
		return java.util.Arrays.stream(values).map(BigDecimal::new).toList();
	}

	public record Sample(
			BigDecimal arrivalRate,
			BigDecimal timeout,
			BigDecimal time,
			double probabilitySum,
			double buffer0Probability,
			double buffer1Probability,
			double buffer2Probability,
			double buffer3Probability,
			double brokerBusyProbability,
			double timerActiveProbability,
			double averageBuffer,
			double averageInService,
			double effectiveArrivalRate,
			double batchCompletionRate,
			double messageCompletionRate,
			double cumulativeBatches,
			double cumulativeMessages) {

		public static String csvHeader() {
			return "lambda,timeout,B,time,probability_sum,p_buffer_0,p_buffer_1,p_buffer_2,"
					+ "p_buffer_3,p_broker_busy,p_timer_active,average_buffer,average_in_service,"
					+ "effective_arrival_rate,batch_completion_rate,message_completion_rate,"
					+ "cumulative_batches,cumulative_messages";
		}

		public String toCsv() {
			return String.format(java.util.Locale.ROOT,
					"%s,%s,%d,%s,%.12f,%.12f,%.12f,%.12f,%.12f,%.12f,%.12f,%.12f,%.12f,"
							+ "%.12f,%.12f,%.12f,%.12f,%.12f",
					arrivalRate.toPlainString(), timeout.toPlainString(),
					KafkaBrokerModel.FULL_BATCH_SIZE, time.toPlainString(), probabilitySum,
					buffer0Probability, buffer1Probability, buffer2Probability, buffer3Probability,
					brokerBusyProbability, timerActiveProbability, averageBuffer, averageInService,
					effectiveArrivalRate, batchCompletionRate, messageCompletionRate,
					cumulativeBatches, cumulativeMessages);
		}
	}
}
