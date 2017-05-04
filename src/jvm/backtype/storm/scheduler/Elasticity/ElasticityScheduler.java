package backtype.storm.scheduler.Elasticity;

import backtype.storm.scheduler.*;
import backtype.storm.scheduler.Elasticity.MsgServer.MsgServer;
import backtype.storm.scheduler.Elasticity.Strategies.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class ElasticityScheduler implements IScheduler {
	private static final Logger LOG = LoggerFactory
			.getLogger(ElasticityScheduler.class);
	static int flag = 0; // Todo : remove this later. this is to make sure we do scale out only once
	@SuppressWarnings("rawtypes")
	private Map _conf;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf) {
		_conf = conf;
	}

	@Override
	public void schedule(Topologies topologies, Cluster cluster) {
		LOG.info("\n\n\nRerunning ElasticityScheduler...");

		/**
		 * Starting msg server
		 */
		MsgServer msgServer = MsgServer.start(5001);

		/**
		 * Get Global info
		 */
		GlobalState globalState = GlobalState
				.getInstance("ElasticityScheduler");
		globalState.updateInfo(cluster, topologies);

		LOG.info("Global State:\n{}", globalState);

		/**
		 * Get stats
		 */
		GetStats stats = GetStats.getInstance("ElasticityScheduler");
		stats.getStatistics();
		//LOG.info(stats.printTransferThroughputHistory());
		//LOG.info(stats.printEmitThroughputHistory());
		//LOG.info(stats.printExecuteThroughputHistory());
		/**
		 * Start hardware monitoring server
		 */
		Master server = Master.getInstance();
		//server.printStats();
//		Whatever what = new Whatever();
//		what.getSchedule();

		LOG.info("Global State All Nodes : {}", globalState.toString());
		/**
		 * Start Scheduling
		 */
		for (TopologyDetails topo : topologies.getTopologies()) {

			globalState.logTopologyInfo(topo);
			String status = HelperFuncs.getStatus(topo.getId());
			LOG.info("status: {}", status);

			//MsgServer.Signal signal = MsgServer.Signal.ScaleOut;        //msgServer.getMessage();
            MsgServer.Signal signal = msgServer.getMessage();
			if (signal !=null) {

				LOG.info("Signal is: {}", signal.name());
			}

			if(signal == MsgServer.Signal.ScaleOut || (globalState.rebalancingState == MsgServer.Signal.ScaleOut && status.equals("REBALANCING"))){
				LOG.info("SCALEOUT");
				this.scaleOut(msgServer, topo, topologies, globalState, stats, cluster);
				globalState.rebalancingState = MsgServer.Signal.ScaleOut;
			} else if (signal == MsgServer.Signal.ScaleIn) {
				LOG.info("/*** Scaling In ***/");
				StellaInStrategy si = new StellaInStrategy(globalState, stats, topo, cluster, topologies);
				//Node n = si.StrategyScaleIn();
				TreeMap<Node, Integer> rankMap = si.StrategyScaleInAll();

				
				ScaleInTestStrategy strategy = new ScaleInTestStrategy(globalState, stats, topo, cluster, topologies, rankMap);
				//strategy.removeNodeByHostname("pc345.emulab.net");
				//strategy.removeNodeBySupervisorId(n.supervisor_id);
				Map<WorkerSlot, List<ExecutorDetails>> schedMap = strategy
						.getNewScheduling();
				LOG.info("SchedMap: {}", schedMap);
				if (schedMap != null) {
					cluster.freeSlots(schedMap.keySet());
					for (Map.Entry<WorkerSlot, List<ExecutorDetails>> sched : schedMap
							.entrySet()) {
						cluster.assign(sched.getKey(),
								topo.getId(), sched.getValue());
						LOG.info("Assigning {}=>{}",
								sched.getKey(), sched.getValue());
					}
				}
				
				globalState.rebalancingState = MsgServer.Signal.ScaleIn;
			} else {
				LOG.info("ID: {} NAME: {}", topo.getId(), topo.getName());
				LOG.info("Unassigned Executors for {}: ", topo.getName());

				for (Map.Entry<ExecutorDetails, String> k : cluster
						.getNeedsSchedulingExecutorToComponents(topo)
						.entrySet()) {
					LOG.info("{} -> {}", k.getKey(), k.getValue());
				}

				LOG.info("running EvenScheduler now...");
				new backtype.storm.scheduler.EvenScheduler().schedule(
						topologies, cluster);

				globalState.storeState(cluster, topologies);
				globalState.isBalanced = false;
			}

			LOG.info("Current Assignment: {}",
					HelperFuncs.nodeToTask(cluster, topo.getId()));
		}
		if (topologies.getTopologies().size() == 0) {
			globalState.clearStoreState();
		}

	}
	
	public void scaleOut(MsgServer msgServer, TopologyDetails topo, Topologies topologies, GlobalState globalState, GetStats stats, Cluster cluster) {
		String status = HelperFuncs.getStatus(topo.getId());
		LOG.info("Status: {}", status);
		if (!status.equals("REBALANCING")) { //(msgServer.isRebalance() == true) {

			if (true) { //(globalState.stateEmpty() == false) {
				 List<Node> newNodes = globalState.getNewNode();

				LOG.info("New Nodes size : {}", newNodes.size());
				if (true) {
					this.flag++;
					LOG.info("Increasing parallelism...");
					StellaOutStrategy2 strategy = new StellaOutStrategy2(globalState, stats, topo, cluster, topologies);
					HashMap<Component, Integer> compMap = strategy.StellaStrategy(new HashMap<String, Component>());
					
					HelperFuncs.changeParallelism2(compMap, topo);
				}

			}
		} else if (status.equals("REBALANCING")) {
			if (globalState.isBalanced == false) {
				LOG.info("Rebalancing...{}=={}", cluster
						.getUnassignedExecutors(topo).size(), topo
						.getExecutors().size());
				if (cluster.getUnassignedExecutors(topo).size() == topo
						.getExecutors().size()) {
					if (globalState.stateEmpty() == false) {
						LOG.info("Unassigned executors: {}", cluster.getUnassignedExecutors(topo));
						LOG.info("Making migration assignments...");
						globalState.schedState.get(topo.getId());

						IncreaseParallelism strategy = new IncreaseParallelism(
								globalState, stats, topo, cluster,
								topologies);
						Map<WorkerSlot, List<ExecutorDetails>> schedMap = strategy
								.getNewScheduling();
						LOG.info("SchedMap: {}", schedMap);
						if (schedMap != null) {
							for (Map.Entry<WorkerSlot, List<ExecutorDetails>> sched : schedMap
									.entrySet()) {
								cluster.assign(sched.getKey(),
										topo.getId(), sched.getValue());
								LOG.info("Assigning {}=>{}",
										sched.getKey(), sched.getValue());
							}
						}

					}

					globalState.isBalanced = true;
				}
			}
		
		}
	}
}
