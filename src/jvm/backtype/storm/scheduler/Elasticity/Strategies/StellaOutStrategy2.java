package backtype.storm.scheduler.Elasticity.Strategies;

import java.util.*;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.Elasticity.*;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.Elasticity.Strategies.TopologyHeuristicStrategy.ComponentComparator;

/***
 * rank percentage of effect of each node
 * @author Le
 */
public class StellaOutStrategy2 extends TopologyHeuristicStrategy {

    HashMap<String, Double> perExecRate = new HashMap<String, Double>();
    HashMap<String, Double> perCpuRate = new HashMap<String, Double>();
    HashMap<String, Double> throughputToExecuteRatio = new HashMap<String, Double>();

    HashMap<String, Double> ExpectedEmitRateMap = new HashMap<String, Double>();
    HashMap<String, Double> TempExpectedEmitRateMap = new HashMap<String, Double>();

    HashMap<String, Double> ExpectedExecuteRateMap = new HashMap<String, Double>();
    HashMap<String, Double> TempExpectedExecuteRateMap = new HashMap<String, Double>();

    HashMap<String, Double> EmitRateMap = new HashMap<String, Double>();
    HashMap<String, Double> ExecuteRateMap = new HashMap<String, Double>();
    HashMap<String, Integer> ParallelismMap = new HashMap<String, Integer>();

    HashMap<String, Double> CpuMap = new HashMap<String, Double>();
    HashMap<String, Double> ExpectedCpuMap = new HashMap<String, Double>();
    HashMap<String, Double> TempExpectedCpuMap = new HashMap<String, Double>();

    HashMap<ExecutorDetails, Node> ExecToNodeMap = new HashMap<ExecutorDetails, Node>();
    ArrayList<Component> sourceList = new ArrayList<Component>();
    double THRESHOLD_CPU = 0.9;
    int sourceCount;

    int count;

    public StellaOutStrategy2(GlobalState globalState, GetStats getStats,
                             TopologyDetails topo, Cluster cluster, Topologies topologies) {
        super(globalState, getStats, topo, cluster, topologies);
        count=topo.getExecutors().size()/this._cluster.getSupervisors().size();
        LOG.info("NUMBER OF EXECUTORS WE'RE GOING TO ADD: {}", count);
    }

    @Override
    public TreeMap<Component, Integer> Strategy(Map<String, Component> map) {

        init(map);
        HashMap<Component, Integer> arr=new HashMap<Component, Integer>();
        arr=StellaStrategy(map);
        HashMap<Component, Integer> rankMap = new HashMap<Component, Integer>();
        ComponentComparator bvc =  new ComponentComparator(rankMap);
        TreeMap<Component, Integer> IORankMap = new TreeMap<Component, Integer>(bvc);
        for(Map.Entry<Component,Integer> e:arr.entrySet()) {
            rankMap.put(e.getKey(), 1);
        }
        IORankMap.putAll(rankMap);
        return IORankMap;
    }

    private void GetExecToNodeMap() {

        for(Map.Entry<String, Node> node : this._globalState.nodes.entrySet()) {
            LOG.info("Node: {}", node);
            for (ExecutorDetails exec : node.getValue().execs) {
                LOG.info("ExecutorDetails: {}", exec);
                this.ExecToNodeMap.put(exec, node.getValue());
            }
        }
        LOG.info("execToNodeMap: {}", this.ExecToNodeMap.toString());
    }

    private void GetCpuMap() {
        LOG.info("Enter()");

        double avg = 0;
        for(Map.Entry<String, Node> host : this._globalState.nodes.entrySet()) {
            avg = 0.1;
            String hostname = host.getValue().hostname;
            LOG.info("Checking hostname: {}", hostname);
            if (this._getStats.cpuHistory.get(hostname) == null) {
                LOG.info("Cpu history entry for {} is null", host.getKey());
                LOG.info("Cpu history map: {}", this._getStats.cpuHistory);
            } else {

                for (Profile prof : this._getStats.cpuHistory.get(hostname)) {
                    if (prof != null) {
                        avg = prof.getCpu_usage();
                        LOG.info("Profile not null, avg: {}", avg);
                    }
                }


            }

            if (avg == 0) avg = 0.1;
            this.CpuMap.put(host.getKey(), avg);
            LOG.info("Cpu Host Map size: {}", this.CpuMap.size());
        }
    }


    private void init(Map<String, Component> map) {
        // TODO Auto-generated method stub
        this.EmitRateMap = new HashMap<String, Double>();

        for( Map.Entry<String, HashMap<String, List<Integer>>> i : this._getStats.emitThroughputHistory.entrySet()) {
            LOG.info("Topology: {}", i.getKey());
            for(Map.Entry<String, List<Integer>> k : i.getValue().entrySet()) {
				/*LOG.info("Component: {}", k.getKey());
				LOG.info("Emit History: ", k.getValue());
				LOG.info("MvgAvg: {}", HelperFuncs.computeMovAvg(k.getValue()));*/
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
				/*LOG.info("Component: {}", k.getKey());
				LOG.info("Execute History: ", k.getValue());
				LOG.info("MvgAvg: {}", HelperFuncs.computeMovAvg(k.getValue()));*/
                this.ExecuteRateMap.put(k.getKey(), HelperFuncs.computeMovAvg(k.getValue()));
                LOG.info("77- ExectureRateMap Key is {}", k.getKey());
            }
        }
        LOG.info("Execute Rate: {}", ExecuteRateMap);
        this.ExpectedExecuteRateMap.putAll(ExecuteRateMap);

        //parallelism map
        this.ParallelismMap = new HashMap<String, Integer>();
        for( Map.Entry<String, HashMap<String, List<Integer>>> i : this._getStats.emitThroughputHistory.entrySet()) {
            LOG.info("Topology: {}", i.getKey());
            for(Map.Entry<String, List<Integer>> k : i.getValue().entrySet()) {
                Component self=this._globalState.components.get(this._topo.getId()).get(k.getKey());
                LOG.info("Component: {}", self.id);
                LOG.info("parallelism level: {}", self.execs.size());
                this.ParallelismMap.put(self.id, self.execs.size());
            }
        }

        //source list, in case we need to speed up the entire thing
        this.sourceList=new ArrayList<Component>();
        for( Map.Entry<String, Double> i : EmitRateMap.entrySet()) {
            Component self=this._globalState.components.get(this._topo.getId()).get(i.getKey());
            if(self.parents.size()==0){
                this.sourceList.add(self);
            }
        }

        // HashMap<String, Integer> perExecRate = new HashMap<String, Integer>();
        // HashMap<String, Integer> perCpuRate = new HashMap<String, Integer>();
        // HashMap<String, Double> throughputToExecuteRatio;
        this.perExecRate = new HashMap<String, Double>();
        for(HashMap.Entry<String, Double> entry : this.ExecuteRateMap.entrySet()) {
            this.perExecRate.put(entry.getKey(), entry.getValue()/this.ParallelismMap.get(entry.getKey()));
        }

        // right now it is doing emit. Should be doing Transfer?
        this.throughputToExecuteRatio = new HashMap<String, Double>();
        for(HashMap.Entry<String, Double> entry : this.EmitRateMap.entrySet()) {
            this.throughputToExecuteRatio.put(entry.getKey(), entry.getValue()/this.ExecuteRateMap.get(entry.getKey()));
        }

        // cc: place holder
        this.GetCpuMap();

        // get tuple per cpu
        this.perCpuRate = new HashMap<String, Double>();
        for(Map.Entry<String, Node> host : this._globalState.nodes.entrySet()) {
//            LOG.info("77- Key in globalstate.nodes {}, Node ID: {}", host.getKey(), host.getValue().supervisor_id);
//            LOG.info("NodeStats {}", this._getStats.nodeStats);
//            LOG.info("hostname {}", host.getValue().hostname);
//            LOG.info("2 {}, ",this._getStats.nodeStats.get(host.getValue().hostname).emit_throughput );
//            LOG.info("3 {}, ", this.CpuMap.get(host.getValue().hostname));
//            LOG.info("CPU Map {}", this.CpuMap);
            this.perCpuRate.put(host.getValue().hostname, ( this._getStats.nodeStats.get(host.getValue().hostname).emit_throughput / this.CpuMap.get(host.getValue().supervisor_id)));
            LOG.info("perCpu Rate for {} : {}", host.getValue().hostname, this.perCpuRate.get(host.getValue().hostname));
        }


        this.GetExecToNodeMap();

        this.sourceCount=0;

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

    private boolean IsComponentCongested(Component comp) {
        LOG.info("In isComponentCongested");

        boolean ret = true;
        for (ExecutorDetails exec : comp.execs) {
            Node node = this.ExecToNodeMap.get(exec);
            double currentCPu = this.CpuMap.get(node.supervisor_id);
            if(currentCPu < THRESHOLD_CPU) {
                ret = false;
                LOG.info("Node: {} Cpu: {}", node, currentCPu);
                break;
            }
        }
        LOG.info("Component Congested {}", ret);
        return ret;
    }

    private boolean getExpectedImprovement(Component head) {
        LOG.info("In getExpectedImprovmeent");

        // adjust head
        /*double expectedThroughputIncrease = this.perExecRate.get(head.id) * this.throughputToExecuteRatio.get(c.id);
        double executeIncrease = this.TempExpectedExecuteRateMap.get(head.id) + this.perExecRate.get(head.id);
        this.TempExpectedExecuteRateMap.put(head.id, executeIncrease);
        double emitIncrease = this.TempExpectedExecuteRateMap.get(head.id) * this.throughputToExecuteRatio.get(head.id);
        this.TempExpectedEmitRateMap.put(head.id, emitIncrease);
        */
//
        double expectedThroughputIncrease = this.perExecRate.get(head.id) * this.throughputToExecuteRatio.get(head.id);
        this.TempExpectedEmitRateMap.put(head.id, expectedThroughputIncrease);

        LOG.info("Component Id: {}", head.id);
        LOG.info("executeToThroughputRatio : {}, TempExpectedEmitIncrease : {}",
                this.throughputToExecuteRatio.get(head.id), expectedThroughputIncrease);
        LOG.info("Current Rate : {}, Throughput : {}",
                this.ExecuteRateMap.get(head.id) ,this.EmitRateMap.get(head.id));

        // push all children
        LinkedList<Component> queue = new LinkedList<Component>();
        if(head.children.size() != 0)
        for (String child : head.children) {
            Map<String, Component> compMap = this._globalState.components.get(this._topo.getId());
            LOG.info("Adding Child : {}", compMap.get(child));
            queue.add(compMap.get(child));
        }

        /* first get some component stats */
        while (queue.size() != 0) {
            Component c = queue.remove();
            LOG.info("Component : {}", c.id);

            // get parent increase assuming equal distribution among children
            double parent_increase = 0;
            for (String p : c.parents) {
                Component parent = this._globalState.components.get(this._topo.getId()).get(p);
                parent_increase += this.TempExpectedEmitRateMap.get(parent.id) / parent.children.size();
            }
            LOG.info("Expected Parent Increase : {}", parent_increase);

            // calculate component increase
            double new_parent_increase = 0;
            for (ExecutorDetails executorDetails : c.execs) {
                Node node = this.ExecToNodeMap.get(executorDetails);

                double cpu_increase = parent_increase / this.perCpuRate.get(node.hostname);
                LOG.info("Expected Cpu Increase : {}", cpu_increase);
                if (cpu_increase + this.CpuMap.get(node.supervisor_id) > THRESHOLD_CPU) {
                    cpu_increase = THRESHOLD_CPU - this.CpuMap.get(node.supervisor_id);
                    new_parent_increase = cpu_increase * this.perCpuRate.get(node.hostname);
                }

                LOG.info("cpu for node : {} increase : {}", node.hostname, cpu_increase);
            }

            parent_increase = new_parent_increase;
            LOG.info("throughput increase : {}",
                    parent_increase * this.throughputToExecuteRatio.get(c.id));

            this.TempExpectedExecuteRateMap.put(c.id, parent_increase);
            this.TempExpectedEmitRateMap.put(c.id, parent_increase * this.throughputToExecuteRatio.get(c.id));

            if(c.children.size() != 0)
                for (String child : c.children) {
                    Map<String, Component> compMap = this._globalState.components.get(this._topo.getId());
                    LOG.info("Adding Child : {}", compMap.get(child));
                    queue.add(compMap.get(child));
                }
        }

        return false;
    }

    public HashMap<Component, Integer> StellaStrategy(Map<String, Component> map) {
        // TODO Auto-generated method stub
        //construct a map for emit throughput for each component
        init(map);
        HashMap<Component, Integer> ret=new HashMap<Component, Integer>();

        for(int j=0;j<count;j++){
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

                // it should be true == isCongested. This is for testing, we want to add all components as congested
                if(false == this.IsComponentCongested(self)){
                    Double io=in-out;
                    IOMap.put(i.getKey(), io);
                    LOG.info("component: {} IO overflow: {}", i.getKey(), io);
                }
//                if(in>1.2*out){
//                    Double io=in-out;
//                    IOMap.put(i.getKey(), io);
//                    LOG.info("component: {} IO overflow: {}", i.getKey(), io);
//                } //TODO Get rid of
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
            if(total_throughput==0.0){
                LOG.info("No throughput!");
                continue;//no analysis
            }
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
            //find component with max EETP
            Double max=0.0;
            Component top=null;
            for(Map.Entry<Component, Integer> e: rankMap.entrySet()){
                LOG.info("Enter For loop for EETP()");
                //Todo : undo this comment if(this.ParallelismMap.get(e.getKey().id)>=findTaskSize(e.getKey()))//cant exceed the threshold
                    // continue;
                LOG.info("Enter EETP()");
                Integer outpercentage=e.getValue();
                this.getExpectedImprovement(e.getKey());
                Integer increase = this.getExpectedThroughput(SinkMap);
                LOG.info("Projected Increase : {}", increase);

                Double improve_potential=outpercentage/(double)this.ParallelismMap.get(e.getKey().id);
                if(improve_potential>=max){
                    top=e.getKey();
                    max=improve_potential;
                }
            }
            if(top!=null){
                LOG.info("TOP OF {} ITERATION: {}", j, top);
                if(ret.containsKey(top)==false){//if top is not in the return map yet
                    ret.put(top, 1);
                }
                else{
                    ret.put(top, ret.get(top)+1);
                }
                //update throughput map
                //update exectue rate map
//                this.updateExpectedRateChange();
                int current_pllsm=this.ParallelismMap.get(top.id);
                ExpectedExecuteRateMap.put(top.id, (current_pllsm+1)/(double)(current_pllsm)*ExpectedExecuteRateMap.get(top.id));
                //update emit rate map
                ExpectedEmitRateMap.put(top.id, (current_pllsm+1)/(double)(current_pllsm)*ExpectedEmitRateMap.get(top.id));
                //update parallelism map
                this.ParallelismMap.put(top.id, current_pllsm+1);
            }
            else{
                Component current_source=this.sourceList.get(sourceCount%(this.sourceList.size()));
                if(ret.containsKey(current_source)==false){//if top is source in the return map yet
                    ret.put(current_source, 1);
                }
                else{
                    ret.put(current_source, ret.get(current_source)+1);
                }
            }
        }

        ret=new HashMap<Component, Integer>();
        LOG.info("List of components that need to be parallelized:{}",ret);
        return ret;
    }

    private int getExpectedThroughput(HashMap<String, Double> sinkmap) {
        int increase = 0;

        for (HashMap.Entry<String, Double> compStr : sinkmap.entrySet()) {
            increase += TempExpectedEmitRateMap.get(compStr.getKey());
        }

        return increase;
    }

    private void updateExpectedRateChange() {
        LOG.info("in updateExpectedRateChange");
    }

    private Integer findTaskSize(Component key) {
        // TODO Auto-generated method stub
        Integer ret=0;
        for(int i=0; i<key.execs.size();i++){
            ret=ret + key.execs.get(i).getEndTask() - key.execs.get(i).getStartTask()+1;
        }
        return ret;
    }
}