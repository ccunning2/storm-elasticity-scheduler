package backtype.storm.scheduler.Elasticity.Strategies;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.Elasticity.Component;
import backtype.storm.scheduler.Elasticity.GetStats;
import backtype.storm.scheduler.Elasticity.GlobalState;
import backtype.storm.scheduler.Elasticity.HelperFuncs;
import backtype.storm.scheduler.Elasticity.Strategies.TopologyHeuristicStrategy.ComponentComparator;

/***
 * rank percentage of effect of each node
 * @author Le
 */
public class StellaTwoStrategy extends TopologyHeuristicStrategy {
	
	HashMap<String, Double> ExpectedEmitRateMap = new HashMap<String, Double>();
	HashMap<String, Double> ExpectedExecuteRateMap = new HashMap<String, Double>();
	HashMap<String, Double> EmitRateMap = new HashMap<String, Double>();
	HashMap<String, Double> ExecuteRateMap = new HashMap<String, Double>();
	int count=4;
	
	public StellaTwoStrategy(GlobalState globalState, GetStats getStats,
			TopologyDetails topo, Cluster cluster, Topologies topologies) {
		super(globalState, getStats, topo, cluster, topologies);
	}

	@Override
	public TreeMap<Component, Integer> Strategy(Map<String, Component> map) {
		
		init(map);
		ArrayList<Component> arr=new ArrayList<Component>();
		arr=StellaStrategy(map);
		HashMap<Component, Integer> rankMap = new HashMap<Component, Integer>();
		ComponentComparator bvc =  new ComponentComparator(rankMap);
		TreeMap<Component, Integer> IORankMap = new TreeMap<Component, Integer>(bvc);
		for(int i=0;i<arr.size();i++){
			rankMap.put(arr.get(i), 1);
		}
		IORankMap.putAll(rankMap);
		return IORankMap;
	}

	
	
	private void init(Map<String, Component> map) {
		// TODO Auto-generated method stub
		this.EmitRateMap = new HashMap<String, Double>();
		for( Map.Entry<String, HashMap<String, List<Integer>>> i : this._getStats.emitThroughputHistory.entrySet()) {
			LOG.info("Topology: {}", i.getKey());
			for(Map.Entry<String, List<Integer>> k : i.getValue().entrySet()) {
				LOG.info("Component: {}", k.getKey());
				LOG.info("Emit History: ", k.getValue());
				LOG.info("MvgAvg: {}", HelperFuncs.computeMovAvg(k.getValue()));
				this.EmitRateMap.put(k.getKey(), HelperFuncs.computeMovAvg(k.getValue()));
			}
		}
		LOG.info("Emit Rate: {}", EmitRateMap);
		this.ExpectedEmitRateMap.putAll(EmitRateMap);
		//construct a map for emit throughput for each component
		this.ExecuteRateMap = new HashMap<String, Double>();
		for( Map.Entry<String, HashMap<String, List<Integer>>> i : this._getStats.executeThroughputHistory.entrySet()) {
			LOG.info("Topology: {}", i.getKey());
			for(Map.Entry<String, List<Integer>> k : i.getValue().entrySet()) {
				LOG.info("Component: {}", k.getKey());
				LOG.info("Execute History: ", k.getValue());
				LOG.info("MvgAvg: {}", HelperFuncs.computeMovAvg(k.getValue()));
				this.ExecuteRateMap.put(k.getKey(), HelperFuncs.computeMovAvg(k.getValue()));
			}
		}
		LOG.info("Execute Rate: {}", ExecuteRateMap);
		this.ExpectedExecuteRateMap.putAll(ExecuteRateMap);
	}

	private Double RecursiveFind(Component self, HashMap<String, Double> sinkMap, HashMap<String, Double> iOMap) {
		// TODO Auto-generated method stub
		if(self.children.size()==0){
			return sinkMap.get(self.id);//this branch leads to a final value with no overflowed node between
		}
		Double sum=0.0;
		for (int i=0; i<self.children.size();i++){
			if(iOMap.get(self.children.get(i))!=null){//if child is also overflowed, return 0 on this branch
				continue;//ignore this branch move forward
			}
			else{
				Component child=this._globalState.components.get(this._topo.getId()).get(self.children.get(i));//lookup child's component
				sum+=RecursiveFind(child,sinkMap,iOMap);
			}	
		}
		return sum;
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

	
	public ArrayList<Component> StellaStrategy(Map<String, Component> map) {
		// TODO Auto-generated method stub
		//construct a map for emit throughput for each component
		init(map);
		ArrayList<Component> ret=new ArrayList<Component>();
		
		for(int j=0;j<4;j++){
			LOG.info("ROUND {}", j);
			//construct a map for in-out throughput for each component
			HashMap<String, Double> IOMap = new HashMap<String, Double>();
			ComponentComparatorDouble bvc1 = new ComponentComparatorDouble(IOMap);
			TreeMap<String, Double> IORankMap = new TreeMap<String, Double>(bvc1);
			for( Map.Entry<String, Double> i : ExpectedExecuteRateMap.entrySet()) {
				Double out=i.getValue();
				Double in=0.0;
				Component self=this._globalState.components.get(this._topo.getId()).get(i.getKey());
				if(self.parents.size()!=0){
					for(String parent: self.parents){
						in+=ExpectedEmitRateMap.get(parent);
					}
				}
				if(in>1.2*out){
					Double io=in-out;
					IOMap.put(i.getKey(), io);
					LOG.info("component: {} IO overflow: {}", i.getKey(), io);
				}	
			}
			IORankMap.putAll(IOMap);
			LOG.info("overload map", IOMap);	
			//find all output bolts and their throughput
			HashMap<String, Double> SinkMap = new HashMap<String, Double>();
			Double total_throughput=0.0;
			for( Map.Entry<String, Double> i : ExpectedEmitRateMap.entrySet()) {
				Component self=this._globalState.components.get(this._topo.getId()).get(i.getKey());
				if(self.children.size()==0){
					LOG.info("the sink {} has throughput {}", i.getKey(), i.getValue());
					total_throughput+=i.getValue();	
				}
			}
			LOG.info("total throughput: {} ", total_throughput);
			for( Map.Entry<String, Double> i : ExpectedEmitRateMap.entrySet()) {
				Component self=this._globalState.components.get(this._topo.getId()).get(i.getKey());
				if(self.children.size()==0){
					LOG.info("sink: {} throughput percentage: {}", i.getKey(), (i.getValue())/total_throughput);
					SinkMap.put(i.getKey(),(i.getValue())/total_throughput);
				}
			}
			
			//Traverse tree for each node, finding the effective percentage of each component
			/**adding final percentage to the map**/
			HashMap<Component, Integer> rankMap = new HashMap<Component, Integer>();
			for (Map.Entry<String, Double> entry : IOMap.entrySet()) {
				Component self=this._globalState.components.get(this._topo.getId()).get(entry.getKey());
				Double score=RecursiveFind(self,SinkMap,IOMap)*100;
				LOG.info("sink: {} effective throughput percentage: {}", self.id, score);
				rankMap.put(self, score.intValue());
					
			}
		
			Integer max=0;
			Component top=null;
			for(Map.Entry<Component, Integer> e: rankMap.entrySet()){
				if(e.getValue()>=max){
					top=e.getKey();
					max=e.getValue();
				}					
			}
			if(top!=null){
				LOG.info("TOP OF {} ITERATION: {}", j, top);
				ret.add(top);
				//update throughput map 
				//update exectue rate map
				ExpectedExecuteRateMap.put(top.id, 1.25*ExpectedExecuteRateMap.get(top.id));
				//update emit rate map
				ExpectedEmitRateMap.put(top.id, 1.25*ExpectedEmitRateMap.get(top.id));
			}			
		}
		LOG.info("List of components that need to be parallelized:{}",ret);
		return ret;
	}
}
