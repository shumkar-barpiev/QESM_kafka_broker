package com.myexam.qesm.kafka;

import java.math.BigDecimal;
import org.oristool.models.pn.Priority;
import org.oristool.models.stpn.MarkingExpr;
import org.oristool.models.stpn.trees.StochasticTransitionFeature;
import org.oristool.petrinet.Marking;
import org.oristool.petrinet.PetriNet;
import org.oristool.petrinet.Place;
import org.oristool.petrinet.Transition;

public class KafkaBrokerModel {
	public static void build(PetriNet net, Marking marking) {

		// Generating Nodes
		Place P_BatchService = net.addPlace("P_BatchService");
		Place P_BrokerBusy = net.addPlace("P_BrokerBusy");
		Place P_BrokerIdle = net.addPlace("P_BrokerIdle");
		Place P_Buffer = net.addPlace("P_Buffer");
		Place P_TimeoutExpired = net.addPlace("P_TimeoutExpired");
		Place P_TimerActive = net.addPlace("P_TimerActive");
		Place P_Timeroff = net.addPlace("P_Timeroff");
		Transition ArrivalAccepted = net.addTransition("ArrivalAccepted");
		Transition T_DispatchFullActive = net.addTransition("T_DispatchFullActive");
		Transition T_DispatchFullExpired = net.addTransition("T_DispatchFullExpired");
		Transition T_DispatchPartial_1 = net.addTransition("T_DispatchPartial_1");
		Transition T_DispatchPartial_2 = net.addTransition("T_DispatchPartial_2");
		Transition T_Send1 = net.addTransition("T_Send1");
		Transition T_Send2 = net.addTransition("T_Send2");
		Transition T_Send3 = net.addTransition("T_Send3");
		Transition T_StartTimeout = net.addTransition("T_StartTimeout");
		Transition T_Timeout = net.addTransition("T_Timeout");

		// Generating Connectors
		net.addPostcondition(T_DispatchPartial_1, P_BatchService);
		net.addPrecondition(P_Timeroff, T_StartTimeout);
		net.addPostcondition(T_Timeout, P_TimeoutExpired);
		net.addPrecondition(P_Buffer, T_DispatchFullActive);
		net.addPrecondition(P_Buffer, T_DispatchPartial_1);
		net.addPrecondition(P_BrokerIdle, T_DispatchPartial_2);
		net.addPrecondition(P_TimerActive, T_Timeout);
		net.addPrecondition(P_TimerActive, T_DispatchPartial_2);
		net.addPrecondition(P_Buffer, T_DispatchPartial_2);
		net.addPrecondition(P_TimeoutExpired, T_DispatchPartial_1);
		net.addPrecondition(P_TimeoutExpired, T_DispatchFullExpired);
		net.addPostcondition(T_StartTimeout, P_TimerActive);
		net.addPostcondition(T_DispatchFullExpired, P_BrokerBusy);
		net.addPrecondition(P_Buffer, T_StartTimeout);
		net.addPostcondition(T_DispatchFullActive, P_BrokerBusy);
		net.addPostcondition(ArrivalAccepted, P_Buffer);
		net.addPrecondition(P_BrokerIdle, T_DispatchPartial_1);
		net.addPrecondition(P_BrokerIdle, T_DispatchFullExpired);
		net.addPrecondition(P_TimerActive, T_DispatchFullActive);
		net.addPostcondition(T_DispatchFullActive, P_BatchService);
		net.addPrecondition(P_BrokerIdle, T_DispatchFullActive);
		net.addPostcondition(T_StartTimeout, P_Buffer);
		net.addPostcondition(T_DispatchFullExpired, P_Timeroff);
		net.addPostcondition(T_DispatchFullActive, P_Timeroff);
		net.addPostcondition(T_DispatchPartial_1, P_BrokerBusy);
		net.addPrecondition(P_Buffer, T_DispatchFullExpired);
		net.addPostcondition(T_DispatchPartial_1, P_Timeroff);
		net.addPostcondition(T_DispatchFullExpired, P_BatchService);
		net.addPostcondition(T_DispatchPartial_2, P_Timeroff);
		net.addPostcondition(T_DispatchPartial_2, P_BrokerBusy);
		net.addPostcondition(T_DispatchPartial_2, P_BatchService);
		net.addPrecondition(P_BrokerBusy, T_Send1);
		net.addPrecondition(P_BatchService, T_Send1);
		net.addPrecondition(P_BrokerBusy, T_Send2);
		net.addPrecondition(P_BatchService, T_Send2);
		net.addPrecondition(P_BrokerBusy, T_Send3);
		net.addPrecondition(P_BatchService, T_Send3);
		net.addPostcondition(T_Send3, P_BrokerIdle);
		net.addPostcondition(T_Send2, P_BrokerIdle);
		net.addPostcondition(T_Send1, P_BrokerIdle);

		// Generating Properties
		marking.setTokens(P_BatchService, 0);
		marking.setTokens(P_BrokerBusy, 0);
		marking.setTokens(P_BrokerIdle, 1);
		marking.setTokens(P_Buffer, 0);
		marking.setTokens(P_TimeoutExpired, 0);
		marking.setTokens(P_TimerActive, 0);
		marking.setTokens(P_Timeroff, 1);
		ArrivalAccepted.addFeature(
				StochasticTransitionFeature.newExponentialInstance(new BigDecimal("1"), MarkingExpr.from("1", net)));
		T_DispatchFullActive.addFeature(
				StochasticTransitionFeature.newDeterministicInstance(new BigDecimal("0"), MarkingExpr.from("1", net)));
		T_DispatchFullActive.addFeature(new Priority(0));
		T_DispatchFullExpired.addFeature(
				StochasticTransitionFeature.newDeterministicInstance(new BigDecimal("0"), MarkingExpr.from("1", net)));
		T_DispatchFullExpired.addFeature(new Priority(0));
		T_DispatchPartial_1.addFeature(
				StochasticTransitionFeature.newDeterministicInstance(new BigDecimal("0"), MarkingExpr.from("1", net)));
		T_DispatchPartial_1.addFeature(new Priority(0));
		T_DispatchPartial_2.addFeature(
				StochasticTransitionFeature.newDeterministicInstance(new BigDecimal("0"), MarkingExpr.from("1", net)));
		T_DispatchPartial_2.addFeature(new Priority(0));
		T_Send1.addFeature(
				StochasticTransitionFeature.newExponentialInstance(new BigDecimal("1"), MarkingExpr.from("1", net)));
		T_Send2.addFeature(
				StochasticTransitionFeature.newExponentialInstance(new BigDecimal("1"), MarkingExpr.from("1", net)));
		T_Send3.addFeature(
				StochasticTransitionFeature.newExponentialInstance(new BigDecimal("1"), MarkingExpr.from("1", net)));
		T_StartTimeout.addFeature(
				StochasticTransitionFeature.newDeterministicInstance(new BigDecimal("0"), MarkingExpr.from("1", net)));
		T_StartTimeout.addFeature(new Priority(0));
		T_Timeout.addFeature(
				StochasticTransitionFeature.newDeterministicInstance(new BigDecimal("1"), MarkingExpr.from("1", net)));
		T_Timeout.addFeature(new Priority(0));
	}
}
