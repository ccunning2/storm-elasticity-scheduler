package backtype.storm.scheduler.Elasticity;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by cameroncunning on 4/23/17.
 */
public class Whatever {

    private static final Logger LOG = LoggerFactory
            .getLogger(Whatever.class);

    public  GetStats stats;
    public GlobalState globalState;

    public void getSchedule() {
        this.stats = GetStats.getInstance("ElasticityScheduler");
        this.globalState = GlobalState.getInstance("ElasticityScheduler");
        updateInputandOutputCpu();
    }

    private  void updateInputandOutputCpu() {
        Map<String, Map<String, Component>> topo_components = this.globalState.components;
        Component head = null;

        for(String topo : topo_components.keySet()){
            Map<String, Component> components = topo_components.get(topo);

            // get head for bfs
            for (Map.Entry<String, Component> entry : components.entrySet()) {
                Component comp = entry.getValue();
                if (comp.parents.size() == 0) {
                    head = comp;
                }
            }

            if (head == null) {
                LOG.error("Unable to get head component");
            }

            // Do bfs to update each component
            LinkedList<Component> queue = new LinkedList<Component>();
            queue.addLast(head);

            while (queue.isEmpty() != true) {
                Component comp = queue.removeFirst();

                // update actual comp stats
                LOG.info("Updating topo {}", topo);
                UpdateComponent(comp, topo);

                // push to queue for bfs
                for (String child : comp.children) {
                    queue.addLast(components.get(child));
                }
            }
        }

        // update node values
        for(Map.Entry<String, Node> node : this.globalState.nodes.entrySet()) {
            LOG.info("Trying to update node {}", node.getValue().hostname);
            UpdateNode(node.getValue());
        }


    }

    private void UpdateComponent(Component component, String topology) {
        int input = 0;
        int output = 0;
        double ioRatio = 0.0;
        int numExecutor = 0;
        int perExecutor = 0;

        LOG.info("Updating component {} in topology {}", component.id, topology);

        for (String parent : component.parents) {
            Component par = this.globalState.components.get(topology).get(parent);
            input += this.stats.componentStats.get(topology).get(par.id).total_transfer_throughput;
        }

        // get input to component
        this.stats.componentStats.get(topology).get(component.id).total_transfer_input = input;

        LOG.info("Component input {}", input);

        // get io ratio
        output = this.stats.componentStats.get(topology).get(component.id).total_transfer_throughput;
        ioRatio = (double)output/input;
        this.stats.componentStats.get(topology).get(component.id).io_ratio = ioRatio;

        LOG.info("Component io ratio {}", ioRatio);

        // executor ratio
        numExecutor = component.execs.size();
        this.stats.componentStats.get(topology).get(component.id).executor_ratio = (double)input/numExecutor;

        LOG.info("Component executor ratio {}", this.stats.componentStats.get(topology).get(component.id).executor_ratio );
    }

    private void UpdateNode(Node node) {
        Master master = Master.getInstance();

        // get cpu
        Profile profile = master.profile_map.get(node.hostname);
        if (profile != null) {
            double cpu = profile.getCpu_usage();
//            if (this.stats.nodeStats.get(node.hostname).cpu != null) {
//                this.stats.nodeStats.get(node.hostname).cpu = cpu;
//                double ratio = this.stats.nodeStats.get(node.hostname).transfer_throughput / cpu;
//                // calculate cpu per tuple (should be using transfer_input)
//                this.stats.nodeStats.get(node.hostname).tuplePerCpu = ratio;
//                LOG.info("Node ratio is {}", ratio);
            }


        }
    }
}


