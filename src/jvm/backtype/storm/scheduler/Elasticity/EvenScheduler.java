package backtype.storm.scheduler.Elasticity;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;

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
		/**
		 * Get Global info
		 */
		GlobalState globalState = GlobalState.getInstance("EvenScheduler");
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
