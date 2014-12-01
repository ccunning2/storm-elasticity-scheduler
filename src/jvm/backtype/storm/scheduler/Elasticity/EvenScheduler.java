package backtype.storm.scheduler.Elasticity;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.scheduler.Elasticity.Strategies.StellaTwoStrategy;

public class EvenScheduler implements IScheduler{
	private static final Logger LOG = LoggerFactory
			.getLogger(EvenScheduler.class);
	@SuppressWarnings("rawtypes")
	private Map _conf;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf) {
		_conf = conf;
	}

	@Override
	public void schedule(Topologies topologies, Cluster cluster) {
		LOG.info("\n\n\nRerunning EvenScheduler...");
		GetStats gs = GetStats.getInstance("EvenScheduler");
		gs.getStatistics();


		//from 
		/**
		 * Get Global info
		 */
		GlobalState globalState = GlobalState.getInstance("EvenScheduler");
		globalState.updateInfo(cluster, topologies);

		LOG.info("Global State:\n{}", globalState);

		/**
		 * Get stats
		 */
		GetStats stats = GetStats.getInstance("EvenScheduler");
		stats.getStatistics();
		LOG.info(stats.printTransferThroughputHistory());
		LOG.info(stats.printEmitThroughputHistory());
		LOG.info(stats.printExecuteThroughputHistory());
		/**
		 * Start hardware monitoring server
		 */
		Master server = Master.getInstance();
		for (TopologyDetails topo : topologies.getTopologies()) {
			StellaTwoStrategy s = new StellaTwoStrategy(
					globalState, stats, topo, cluster,
					topologies);
			TreeMap<Component, Integer> tMap = s.Strategy(null);
		}
		
		//end
		/**
		 * Get Global info
		 */
		//GlobalState globalState = GlobalState.getInstance("EvenScheduler");
		globalState.updateInfo(cluster, topologies);
		globalState.storeState(cluster, topologies);
		LOG.info("Global State:\n{}", globalState);

		for (TopologyDetails topo : topologies.getTopologies()) {
			LOG.info("ID: {} NAME: {}", topo.getId(), topo.getName());
			LOG.info("Unassigned Executors for {}: ", topo.getName());
			LOG.info("Current Assignment: {}", HelperFuncs.nodeToTask(cluster, topo.getId()));

			globalState.logTopologyInfo(topo);
		}

		//Master server = Master.getInstance();
		
		LOG.info("running EvenScheduler now...");
		new backtype.storm.scheduler.EvenScheduler().schedule(topologies, cluster);
	}
}
