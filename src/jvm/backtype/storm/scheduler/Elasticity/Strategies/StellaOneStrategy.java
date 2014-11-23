package backtype.storm.scheduler.Elasticity.Strategies;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.Elasticity.GetStats;
import backtype.storm.scheduler.Elasticity.GlobalState;
import backtype.storm.scheduler.Elasticity.Component;
import backtype.storm.scheduler.Elasticity.HelperFuncs;

/***
 * rank percentage of effect of each node
 * @author Le
 */
public class StellaOneStrategy extends TopologyHeuristicStrategy {

	public StellaOneStrategy(GlobalState globalState, GetStats getStats,
			TopologyDetails topo, Cluster cluster, Topologies topologies) {
		super(globalState, getStats, topo, cluster, topologies);
	}

	@Override
	public TreeMap<Component, Integer> Strategy(Map<String, Component> map) {
		//construct a map for emit throughput for each component
		HashMap<String, Double> EmitRateMap = new HashMap<String, Double>();
		for( Map.Entry<String, HashMap<String, List<Integer>>> i : this._getStats.emitThroughputHistory.entrySet()) {
			LOG.info("Topology: {}", i.getKey());
			for(Map.Entry<String, List<Integer>> k : i.getValue().entrySet()) {
				LOG.info("Component: {}"+k.getKey());
				LOG.info("Emit History: "+k.getValue());
				LOG.info("MvgAvg: {}"+HelperFuncs.computeMovAvg(k.getValue()));
				EmitRateMap.put(k.getKey(), HelperFuncs.computeMovAvg(k.getValue()));
			}
		}
		
		//construct a map for emit throughput for each component
		HashMap<String, Double> ExecuteRateMap = new HashMap<String, Double>();
		for( Map.Entry<String, HashMap<String, List<Integer>>> i : this._getStats.executeThroughputHistory.entrySet()) {
			LOG.info("Topology: {}", i.getKey());
			for(Map.Entry<String, List<Integer>> k : i.getValue().entrySet()) {
				LOG.info("Component: {}"+k.getKey());
				LOG.info("Execute History: "+k.getValue());
				LOG.info("MvgAvg: {}"+HelperFuncs.computeMovAvg(k.getValue()));
				ExecuteRateMap.put(k.getKey(), HelperFuncs.computeMovAvg(k.getValue()));
			}
		}

		//construct a map for in-out throughput for each component
		HashMap<String, Double> IOMap = new HashMap<String, Double>();
		ComponentComparatorDouble bvc1 = new ComponentComparatorDouble(IOMap);
		TreeMap<String, Double> IORankMap = new TreeMap<String, Double>(bvc1);
		for( Map.Entry<String, Double> i : ExecuteRateMap.entrySet()) {
			Double out=i.getValue();
			Double in=0.0;
			Component self=this._globalState.components.get(this._topo.getId()).get(i.getKey());
			if(self.parents!=null){
				for(String parent: self.parents){
					in+=EmitRateMap.get(parent);
				}
			}
			if(in>1.2*out){
				IOMap.put(i.getKey(), in-out);
			}	
		}
		IORankMap.putAll(IOMap);
			
				
		
		/**adding final percentage to the map**/
		HashMap<Component, Integer> rankMap = new HashMap<Component, Integer>();

		ComponentComparator bvc = new ComponentComparator(rankMap);
		TreeMap<Component, Integer> retMap = new TreeMap<Component, Integer>(
				bvc);
		for (Map.Entry<String, Component> entry : map.entrySet()) {
			rankMap.put(entry.getValue(), entry.getValue().children.size()
					+ entry.getValue().parents.size());
		}
		retMap.putAll(rankMap);
		return retMap;
	}

	
	public class ComponentComparatorDouble implements Comparator<String> {

		HashMap<String, Double> base;
	    public ComponentComparatorDouble(HashMap<String, Double> base) {
	        this.base = base;
	    }

	    // Note: this comparator imposes orderings that are inconsistent with equals.    
	    public int compare(String a, String b) {
	        if (base.get(a) >= base.get(b)) {
	            return -1;
	        } else {
	            return 1;
	        } // returning 0 would merge keys
	    }
	}
}
