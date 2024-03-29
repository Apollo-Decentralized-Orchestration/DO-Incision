package at.uibk.dps.di.JIT_C;

import at.uibk.dps.di.properties.PropertyServiceScheduler;
import at.uibk.dps.di.schedulerV2.GraphUtility;
import at.uibk.dps.ee.model.graph.EnactmentGraph;
import at.uibk.dps.ee.model.graph.EnactmentSpecification;
import at.uibk.dps.ee.model.properties.PropertyServiceDependency;
import at.uibk.dps.ee.model.properties.PropertyServiceFunctionUser;
import at.uibk.dps.ee.model.utils.UtilsCopy;
import net.sf.opendse.model.*;

import java.util.*;

public class JIT {

    private Map<Task, Map<Task, Double>> TT;

    /**
     * ----> Configuration
     */

    /**
     * The delay in minutes until the resource is
     * ready (initial boot time, initialization, ...).
     */
    private static final double acquisitionDelay = 1.0;

    /**
     * Time interval of a lease period (a resource
     * is used at least for interval minutes)
     */
    private static final double interval = 10.0;

    /**
     * The transfer times between different tasks.
     */
    private void setupTransferTime(EnactmentSpecification specification){

        TT = new HashMap<>();

        String prefix = "taskNode";

        Map<Task, Double> tmp = new HashMap<>();
        tmp.put(specification.getEnactmentGraph().getVertex(prefix + "2"), 6.0);
        tmp.put(specification.getEnactmentGraph().getVertex(prefix + "3"), 6.0);
        tmp.put(specification.getEnactmentGraph().getVertex(prefix + "4"), 6.0);
        TT.put(specification.getEnactmentGraph().getVertex(prefix + "1"), tmp);

        tmp = new HashMap<>();
        tmp.put(specification.getEnactmentGraph().getVertex(prefix + "5"), 4.0);
        tmp.put(specification.getEnactmentGraph().getVertex(prefix + "6"), 4.0);
        TT.put(specification.getEnactmentGraph().getVertex(prefix + "2"), tmp);

        tmp = new HashMap<>();
        tmp.put(specification.getEnactmentGraph().getVertex(prefix + "6"), 5.0);
        TT.put(specification.getEnactmentGraph().getVertex(prefix + "3"), tmp);

        tmp = new HashMap<>();
        tmp.put(specification.getEnactmentGraph().getVertex(prefix + "7"), 4.0);
        TT.put(specification.getEnactmentGraph().getVertex(prefix + "4"), tmp);

        tmp = new HashMap<>();
        tmp.put(specification.getEnactmentGraph().getVertex(prefix + "8"), 3.0);
        TT.put(specification.getEnactmentGraph().getVertex(prefix + "5"), tmp);

        tmp = new HashMap<>();
        tmp.put(specification.getEnactmentGraph().getVertex(prefix + "8"), 2.0);
        TT.put(specification.getEnactmentGraph().getVertex(prefix + "6"), tmp);

        tmp = new HashMap<>();
        tmp.put(specification.getEnactmentGraph().getVertex(prefix + "8"), 4.0);
        TT.put(specification.getEnactmentGraph().getVertex(prefix + "7"), tmp);

        tmp = new HashMap<>();
        tmp.put(specification.getEnactmentGraph().getVertex(prefix + "9"), 8.0);
        TT.put(specification.getEnactmentGraph().getVertex(prefix + "8"), tmp);

        tmp = new HashMap<>();
        tmp.put(specification.getEnactmentGraph().getVertex(prefix + "9"), 0.0);
        TT.put(specification.getEnactmentGraph().getVertex(prefix + "9"), tmp);
    }

    /**
     * Get the cost of a resource.
     *
     * @param resource resource to get cost from.
     * @return Cost of resource.
     */
    private Double Cost(Resource resource) {
        switch (resource.getId()) {
            case "Vl":
                return 0.04;
            case "Vm":
                return 0.02;
            case "Vs":
                return 0.01;
        }
        System.err.println("Could not find cost for resource " + resource.getId());
        return null;
    }

    /**
     * <---- Configuration
     */

    /**
     * Provisioned VM instance id counter.
     */
    private int id = 1;

    /**
     * Scheduler iteration counter.
     */
    private int iteration = 1;

    /**
     * Variables for scheduled tasks, vm-pool, expected
     * finish time, latest finish time, ...
     */
    private Map<Task, Double> AST = new HashMap<>();
    private Map<Task, Double> XFT = new HashMap<>();
    private Map<Task, Double> XST = new HashMap<>();
    private Map<Task, Double> LFT = new HashMap<>();
    private Map<Task, Double> MET = new HashMap<>();
    private Map<Task, Double> EST = new HashMap<>();
    private List<Schedule> scheduled = new ArrayList<>();
    private List<Schedule> scheduledPrint = new ArrayList<>();
    private Map<Task, Resource> scheduledMapping = new HashMap<>();
    private List<VMPoolEntry> VMPoolStatus = new ArrayList<>();

    /**
     * Preprocess the workflow by merging possible tasks.
     *
     * @param specification of the workflow.
     *
     * @return adapted enactment graph.
     */
    private EnactmentGraph preprocessing(EnactmentSpecification specification) {

        // 4.1 Begin
        EnactmentGraph eGraph = UtilsCopy.deepCopyEGraph(specification.getEnactmentGraph());

        // 4.2 tksstack = tentry
        Stack<Task> tksstack = new Stack<>();
        Collection<Task> entryTasks = new ArrayList<>();
        for(Task t : GraphUtility.getRootNodes(eGraph)) {
            entryTasks.addAll(GraphUtility.getSuccessorTaskNodes(eGraph, t));
        }
        entryTasks.forEach(tksstack::push);

        // 4.3 while tksstack is not empty
        while (!tksstack.isEmpty()) {

            // 4.4 tp = tksstack(front)
            Task tp = tksstack.pop();

            // 4.5 Sc = {tc | tc is the child of tp}
            Collection<Task> Sc = GraphUtility.getSuccessorTaskNodes(eGraph, tp);

            // 4.6 If Cardinality Sc=1 and tc has only one parent tp
            if (Sc.size() == 1 && GraphUtility.getPredecessorTaskNodes(eGraph, Sc.iterator().next()).size() == 1) {

                // 4.7 Replace tp and tc with tp+c
                Task tc = Sc.iterator().next();
                Task merged = PropertyServiceFunctionUser.createUserTask(tp.getId() + "+" + tc.getId(), tc.getType());
                for(Task t: GraphUtility.getPredecessorTaskNodes(eGraph, tp)) {
                    Map<Task, Double> tmp = new HashMap<>();
                    tmp.put(merged, getTTIgnoreDirection(t, tc) + getTTIgnoreDirection(t, tp));
                    if(TT.containsKey(t)) {
                        tmp.putAll(TT.get(t));
                        TT.put(t, tmp);
                    } else {
                        TT.put(t, tmp);
                    }
                }
                for(Task t: GraphUtility.getSuccessorTaskNodes(eGraph, tc)) {
                    Map<Task, Double> tmp = new HashMap<>();
                    tmp.put(merged, getTTIgnoreDirection(t, tc) + getTTIgnoreDirection(t, tp));
                    if(TT.containsKey(t)) {
                        tmp.putAll(TT.get(t));
                        TT.put(t, tmp);
                    } else {
                        TT.put(t, tmp);
                    }
                }


                // 4.8 Set tp+c as the parent of t
                for (Task t : eGraph.getPredecessors(tp)) {
                    PropertyServiceDependency.addDataDependency(t, merged, "key", eGraph);
                }
                for (Task t : eGraph.getSuccessors(tc)) {
                    PropertyServiceDependency.addDataDependency(merged, t, "key", eGraph);
                }
                for (Task t : eGraph.getPredecessors(tc)) {
                    eGraph.removeVertex(t);
                }
                eGraph.removeVertex(tc);
                eGraph.removeVertex(tp);

                // 4.10 Add tp+c to the front of tksstack
                tksstack.push(merged);
            }
            // 4.11 Else
            else {
                // 4.12. Add tps children to the rear of tksstack
                Sc.forEach((t) -> tksstack.add(0, t));
            } // 4.13 End if
        } // 4.14 End While

        // 4.15 End
        return eGraph;
    }

    /**
     * Print vm-pool status
     */
    private void printVMPoolStatus() {
        for(VMPoolEntry entry: VMPoolStatus) {
            System.out.println("VMPOOLSTATUS:\t " + entry.getId() + " | " + entry.getType().getId() + " | " + entry.getStartTime() +
                " | " + entry.getExpecteddIdleStartTime() + " | " + entry.getEndTime());
        }
    }

    /**
     * Print the schedule.
     */
    private void printSchedule() {
        for(Schedule entry: scheduledPrint) {
            System.out.println("SCHEDULE:\t\t" + entry.getTask() + " | " + entry.getVm() + " (" + entry.getInstance() + ") | " + entry.getXst() +
                " | " + entry.getXft());
        }
    }

    /**
     * Get transfer time between two tasks from
     * matrix and ignore task ordering.
     *
     * @param t1 task 1
     * @param t2 task 2
     *
     * @return transfer time between two tasks.
     */
    private double getTTIgnoreDirection(Task t1, Task t2){
        if(TT.containsKey(t1)) {
            Map<Task, Double> tmp = TT.get(t1);
            if(tmp.containsKey(t2)) {
                return tmp.get(t2);
            }
        }
        if (TT.containsKey(t2)) {
            Map<Task, Double> tmp = TT.get(t2);
            if(tmp.containsKey(t1)) {
                return tmp.get(t1);
            }
        }
        return 0.0;
    }

    /**
     * Get transfer time between of two tasks
     *
     * @param t1 task 1
     * @param t2 task 2
     *
     * @return transfer time between two tasks.
     */
    private double getTT(Task t1, Task t2){
        if(t1.getId().equals(t2.getId())) {
            return 0.0;
        }
        return getTTIgnoreDirection(t1, t2);
    }

    /**
     * Get the last task running on a specific instance.
     *
     * @param instance to look for.
     *
     * @return last task running on a specific instance.
     */
    private Task getLastTaskOnInstance(String instance) {
        double max = 0.0;
        Task task = null;
        for(Schedule schedule: scheduled) {
            if(schedule.getInstance().equals(instance) && max < schedule.getXft()) {
                max = schedule.getXft();
                task = schedule.getTask();
            }
        }
        return task;
    }

    /**
     * Get a VMPoolEntry by instance.
     *
     * @param instance to look for.
     *
     * @return VMPoolEntry.
     */
    private VMPoolEntry getEntryByInstance(String instance) {
        for(VMPoolEntry entry: VMPoolStatus) {
            if(entry.getId().equals(instance)) {
                return entry;
            }
        }
        return null;
    }

    /**
     * Calculate the expected start time (XST).
     *
     * @param t task to calculate XST.
     * @param eGraph containing tasks.
     * @param entry VMPoolEntry.
     */
    private void calcXST(Task t, EnactmentGraph eGraph, VMPoolEntry entry) {
        double max = 0.0;
        Collection<Task> parents = GraphUtility.getPredecessorTaskNodes(eGraph, t);
        for(Task parent: parents) {
            double delay = getTT(parent, t);
            if(entry != null) {
                for (Schedule s : scheduled) {
                    if (s.getTask().getId().equals(parent.getId()) && s.getInstance().equals(entry.getId())) {
                        if(entry.getExpecteddIdleStartTime() > XFT.get(parent)) {
                            delay = entry.getExpecteddIdleStartTime();
                            XST.put(t, delay);
                            return;
                        } else {
                            delay = 0.0;
                        }
                    }
                }
            }
            if(max < XFT.get(parent) + delay) {
                max = XFT.get(parent) + delay;
            }
        }
        XST.put(t, max);
    }

    /**
     * Calculate the expected finish time (XFT)
     *
     * @param t task to calculate XFT.
     * @param specification of the graph.
     * @param eGraph containing tasks.
     * @param entry VMPoolEntry.
     */
    private void calcXFT(Task t, EnactmentSpecification specification, EnactmentGraph eGraph, VMPoolEntry entry) {
        Collection<Task> parents = GraphUtility.getPredecessorTaskNodes(eGraph, t);
        double max = 0.0;
        for(Task parent: parents) {
            if(!XFT.containsKey(parent)) {
                calcXFT(parent, specification, eGraph, entry);
            }
            double delay = getTT(t, parent);
            for (Schedule s : scheduled) {
                if (s.getTask().getId().equals(parent.getId()) && s.getInstance().equals(entry.getId())) {
                    if(entry.getExpecteddIdleStartTime() <= XFT.get(parent)) {
                        delay = 0.0;
                    } else {
                        delay = entry.getExpecteddIdleStartTime() - XFT.get(parent);
                    }
                }
            }
            if (max < XFT.get(parent) + delay) {
                max = XFT.get(parent) + delay;
            }
        }
        double ET = getET(specification, t, entry.getType());
        XFT.put(t, max + ET);
    }

    /**
     * Get execution time of a task on a specific resource.
     *
     * @param specification of the workflow.
     * @param t task.
     * @param r resource.
     * @return execution time of task t on resource r.
     */
    private double getET(EnactmentSpecification specification, Task t, Resource r){
        double ET = 0.0;
        if(t.getId().contains("+")) {
            String[] tasks = t.getId().split("\\+");
            for(String task: tasks) {
                Set<Mapping<Task, Resource>> mappings = specification.getMappings().getMappings(specification.getEnactmentGraph().getVertex("taskNode" + task.replaceAll("[^0-9]", "")));
                for(Mapping<Task, Resource> map: mappings) {
                    if(map.getTarget().getId().equals(r.getId())) {
                        ET += PropertyServiceScheduler.getDuration(map);
                    }
                }
            }
        } else {
            Set<Mapping<Task, Resource>> mappings = specification.getMappings().getMappings(t);
            for(Mapping<Task, Resource> map: mappings) {
                if(map.getTarget().getId().equals(r.getId())) {
                    ET += PropertyServiceScheduler.getDuration(map);
                }
            }
        }
        return ET;
    }

    /**
     * Calculate latest finish time (LFT).
     *
     * @param t task.
     * @param eGraph containing tasks.
     */
    private void calcLFT(Task t, EnactmentGraph eGraph) {
        double min = Double.MAX_VALUE;
        Collection<Task> children =  GraphUtility.getSuccessorTaskNodes(eGraph, t);
        for(Task child: children) {
            if(min > LFT.get(child) - MET.get(child) - getTT(child, t)) {
                min = LFT.get(child) - MET.get(child) - getTT(child, t);
            }
        }
        LFT.put(t, min);
    }

    /**
     * The actual scheduler.
     *
     * @param specification of the enactment.
     * @param D deadline.
     */
    public void schedule(EnactmentSpecification specification, double D){

        setupTransferTime(specification);

        EnactmentGraph eGraph = specification.getEnactmentGraph();
        Collection<Task> entryTasksTmp = new ArrayList<>();
        for(Task t : GraphUtility.getRootNodes(eGraph)) {
            entryTasksTmp.addAll(GraphUtility.getSuccessorTaskNodes(eGraph, t));
        }

        // 1. Begin
        // 2. Compute MET_W (Minimum execution time of workflow) using Equations 2, 3, 4 and 11
            // Eq. 2: Minimum execution time of task t_i
            Map<Task, Double> MET_tmp = new HashMap<>();
            for(Task t: eGraph.getVertices()) {
                if(!(t instanceof Communication)){
                    Set<Mapping<Task, Resource>> mappings = specification.getMappings().getMappings(t);
                    double min = Double.MAX_VALUE;
                    for(Mapping<Task, Resource> map: mappings) {
                        double duration = PropertyServiceScheduler.getDuration(map);
                        if(duration < min) {
                            min = duration;
                        }
                    }
                    MET_tmp.put(t, min);
                }
            }

            // Eq. 3: Earliest start time of task t_i
            Stack<Task> nodeStack = new Stack<>();
            for(Task entryTask : entryTasksTmp) {
                EST.put(entryTask, 0.0);
                nodeStack.push(entryTask);
            }
            while (!nodeStack.isEmpty()) {
                Task node = nodeStack.pop();
                Collection<Task> successors = GraphUtility.getSuccessorTaskNodes(eGraph, node);
                for(Task successor : successors) {
                    Collection<Task> predecessors = GraphUtility.getPredecessorTaskNodes(eGraph, successor);
                    double max = 0.0;
                    for(Task predecessor : predecessors) {
                        if(!EST.containsKey(predecessor)) {
                            nodeStack.add(0, node);
                        } else {
                            double tmp = EST.get(predecessor) + MET_tmp.get(predecessor) + getTT(successor, predecessor);
                            if (tmp > max) {
                                max = tmp;
                            }
                        }
                    }
                    EST.put(successor, max);
                    nodeStack.push(successor);
                }
            }


            // Eq. 4: Earliest finish time of task t_i
            Map<Task, Double> EFT = new HashMap<>();
            for(Task t: eGraph.getVertices()) {
                if(!(t instanceof Communication)){
                    EFT.put(t, EST.get(t) + MET_tmp.get(t));
                }
            }

            // Eq. 11: Minimum Execution Time of the Workflow
            double max = 0.0;
            for(Task t: eGraph.getVertices()) {
                if(!(t instanceof Communication)){
                    double tmp = EFT.get(t);
                    if(max < tmp) {
                        max = tmp;
                    }
                }
            }
            double MET_W = max;

        // 3. If D >= MET W
        if(D >= MET_W) {

            // 4. Call Pre-processing(W)
            EnactmentGraph adaptedGraph = preprocessing(specification);
            //EnactmentGraphViewer.view(adaptedGraph);

            // 5. Compute MET, LFT and XET matrices using Equations (2), (8) and (10) respectively
                // Eq. 2: Minimum execution time of task t_i
                for(Task t: adaptedGraph.getVertices()) {
                    if(!(t instanceof Communication)){
                        if(t.getId().contains("+")) {
                            String[] tasks = t.getId().split("\\+");
                            double total = 0.0;
                            for(String task: tasks) {
                                Set<Mapping<Task, Resource>> mappings = specification.getMappings().getMappings(specification.getEnactmentGraph().getVertex("taskNode" + task.replaceAll("[^0-9]", "")));
                                double min = Double.MAX_VALUE;
                                for (Mapping<Task, Resource> map : mappings) {
                                    double dur = PropertyServiceScheduler.getDuration(map);
                                    if (dur < min) {
                                        min = dur;
                                    }
                                }
                                total += min;
                            }
                            MET.put(t, total);
                        }else {
                            Set<Mapping<Task, Resource>> mappings = specification.getMappings().getMappings(t);
                            double min = Double.MAX_VALUE;
                            for (Mapping<Task, Resource> map : mappings) {
                                double dur = PropertyServiceScheduler.getDuration(map);
                                if (dur < min) {
                                    min = dur;
                                }
                            }
                            MET.put(t, min);
                        }
                    }
                }

                // Eq. 8: Latest finish time of task t_i
                Stack<Task> stack = new Stack<>();
                Collection<Task> exitTasks = new ArrayList<>();
                for(Task t : GraphUtility.getLeafNodes(adaptedGraph)) {
                    exitTasks.addAll(GraphUtility.getPredecessorTaskNodes(adaptedGraph, t));
                }
                for(Task exitTask : exitTasks) {
                    LFT.put(exitTask, D);
                    stack.push(exitTask);
                }
                while (!stack.isEmpty()) {
                    Task node = stack.pop();
                    Collection<Task> predecessors = GraphUtility.getPredecessorTaskNodes(adaptedGraph, node);
                    for(Task predecessor : predecessors) {
                        Collection<Task> successors = GraphUtility.getSuccessorTaskNodes(adaptedGraph, predecessor);
                        double min = Double.MAX_VALUE;
                        for(Task successor : successors) {
                            if(!LFT.containsKey(successor)) {
                                stack.add(0, node);
                            } else {
                                double tt = getTT(successor, predecessor);
                                double tmp = LFT.get(successor) - MET.get(successor) - tt;
                                if (tmp < min) {
                                    min = tmp;
                                }
                            }
                        }
                        LFT.put(predecessor, min);
                        stack.push(predecessor);
                    }
                }

                // Eq. 10: Expected execution time
                Map<Resource, Map<Task, Double>> XET = new HashMap<>();
                Stack<Task> stackXET = new Stack<>();
                for(Resource r : specification.getResourceGraph().getVertices()) {
                    if(!r.getId().contains("Local")) {
                        Map<Task, Double> tmp = new HashMap<>();
                        for(Task t: exitTasks) {
                            if(t.getId().contains("+")) {
                                String[] tasks = t.getId().split("\\+");
                                double total = 0.0;
                                for(String task: tasks) {
                                    Set<Mapping<Task, Resource>> maps = specification.getMappings().getMappings(
                                        specification.getEnactmentGraph().getVertex("taskNode"+ task.replaceAll("[^0-9]", "")));
                                    for (Mapping<Task, Resource> map : maps) {
                                        if (map.getTarget().getId().equals(r.getId())) {
                                            total += PropertyServiceScheduler.getDuration(map);
                                        }
                                    }
                                }
                                tmp.put(t, total);
                            } else {
                                Set<Mapping<Task, Resource>> maps = specification.getMappings().getMappings(t);
                                for (Mapping<Task, Resource> map : maps) {
                                    if (map.getTarget().getId().equals(r.getId())) {
                                        tmp.put(t, PropertyServiceScheduler.getDuration(map));
                                    }
                                }
                            }
                            stackXET.push(t);
                        }
                        while (!stackXET.isEmpty()) {
                            Task node = stackXET.pop();
                            Collection<Task> predecessors = GraphUtility.getPredecessorTaskNodes(adaptedGraph, node);
                            for(Task predecessor : predecessors) {
                                Collection<Task> successors =
                                    GraphUtility.getSuccessorTaskNodes(adaptedGraph, predecessor);
                                max = 0.0;
                                for (Task successor : successors) {
                                    if (!tmp.containsKey(successor)) {
                                        stackXET.add(0, node);
                                    } else {
                                        if(tmp.get(successor) > max) {
                                            max = tmp.get(successor);
                                        }
                                    }
                                }
                                double et = 0.0;
                                if(predecessor.getId().contains("+")) {
                                    String[] tasks = predecessor.getId().split("\\+");
                                    for(String task: tasks) {
                                        Set<Mapping<Task, Resource>> maps = specification.getMappings().getMappings(
                                            specification.getEnactmentGraph().getVertex("taskNode"+ task.replaceAll("[^0-9]", "")));
                                        for (Mapping<Task, Resource> map : maps) {
                                            if (map.getTarget().getId().equals(r.getId())) {
                                                et += PropertyServiceScheduler.getDuration(map);
                                            }
                                        }
                                    }
                                } else {
                                    Set<Mapping<Task, Resource>> maps = specification.getMappings().getMappings(predecessor);
                                    for (Mapping<Task, Resource> map : maps) {
                                        if (map.getTarget().getId().equals(r.getId())) {
                                            et = PropertyServiceScheduler.getDuration(map);
                                        }
                                    }
                                }
                                tmp.put(predecessor, et + max);
                                stackXET.push(predecessor);
                            }
                        }
                        XET.put(r, tmp);
                    }
                }

            // 6. {t_entry} Root nodes of the workflow graph W
            Collection<Task> entryTasks = new ArrayList<>();
            for(Task t : GraphUtility.getRootNodes(eGraph)) {
                entryTasks.addAll(GraphUtility.getSuccessorTaskNodes(eGraph, t));
            }
            Map<Task, Resource> taskvmmap;


            // 7. For each te 2 {t_entry}
            for(Task t_e : entryTasks) {
                // 8. to_provision CheapesttaskVMMap(t_e)
                    // 8.1. Begin
                    // 8.2. taskvmmap = 0
                    taskvmmap = new HashMap<>();
                    // 8.3. If t is not an entry task then
                    if(!entryTasks.contains(t_e)) {
                        // This will never happen
                    }
                    // 8.14. Else
                    else {
                        // 15. XST = acquisitionDelay
                        XST.put(t_e, acquisitionDelay);
                    }
                    // 8.16. End if
                    // 8.17. Find {VM_k} e VM_set for which XST(t) + XET(t, VM_k) <= D
                    List<Resource> resources = new ArrayList<>();
                    for(Resource res : XET.keySet()) {
                        if(XST.get(t_e) + XET.get(res).get(t_e) <= D) {
                            resources.add(res);
                        }
                    }
                    // 8.18. VM_j = arg(min_VM_k(XET(t,VM_k)/interval * Cost(VM_K)
                    Resource VM_j = null;
                    double min = Double.MAX_VALUE;
                    for(Resource r: resources) {
                        // TODO check paper there is arg(...)
                        double tmp = XET.get(r).get(t_e)/interval * Cost(r);
                        if(min > tmp){
                            min = tmp;
                            VM_j = r;
                        }
                    }
                    // 8.19. taskvmmap = VMj
                    taskvmmap.put(t_e, VM_j);
                Map<Task, Resource> to_provision = taskvmmap;

                // 9. Procure a VM instance ve of type To_Provision from the cloud
                // TODO we assume it is already provisioned
                String instance = "v" + (id++);

                // 10. Schedule t_e on v_e at XST(t_e)
                // LATER

                // 11. Update VM Pool Status
                double ET = getET(specification, t_e, to_provision.get(t_e));
                VMPoolEntry e = new VMPoolEntry(instance, to_provision.get(t_e), 0.0, XST.get(t_e) + ET, null);
                VMPoolStatus.add(e);
                //printVMPoolStatus();

                // 10. Schedule t_e on v_e at XST(t_e)
                calcXFT(t_e, specification, adaptedGraph, e);
                XFT.replace(t_e, XFT.get(t_e) + XST.get(t_e));
                scheduled.add(new Schedule(t_e, to_provision.get(t_e), instance, XST.get(t_e), XFT.get(t_e)));
                scheduledPrint.add(new Schedule(t_e, to_provision.get(t_e), instance, XST.get(t_e), XFT.get(t_e)));
                //printSchedule();

            } // 12. End for



            // 13. While all tasks in T are not completed do
            Map<Task, Boolean> T = new HashMap<>();
            for(Task t: adaptedGraph.getVertices()) {
                if(!(t instanceof Communication)) {
                    T.put(t, false);
                }
            }

            //System.out.println("------->");
            printVMPoolStatus();
            printSchedule();
            //System.out.println("<------");

            while(!T.isEmpty()){

                System.out.println("---------------- iteration " + (iteration++) + " ----------------");

                // 14. Send the scheduled tasks for execution to the execution manager
                while (!scheduledPrint.isEmpty()) {

                    Schedule scheuledTask = scheduledPrint.get(0);
                    System.out.println("\tSend scheduled task " + scheuledTask.getTask().getId() + " to execution manager.");
                    scheduledMapping.put(scheuledTask.getTask(), scheuledTask.getVm());

                    // 15. Update AST (Actual start time), XFT of scheduled tasks
                    AST.put(scheuledTask.getTask(), XST.get(scheuledTask.getTask()));
                    double ET = getET(specification, scheuledTask.getTask(), scheuledTask.getVm());
                    VMPoolEntry entry = getEntryByInstance(scheuledTask.getInstance ());
                    if(entry == null) {
                        XFT.put(scheuledTask.getTask(), AST.get(scheuledTask.getTask()) + ET);
                    }
                    scheuledTask.setXst(XST.get(scheuledTask.getTask()));
                    scheuledTask.setXft(XFT.get(scheuledTask.getTask()));


                    System.out.println("[[[ SIMULATED: " + scheuledTask.getTask().getId() + " finished ]]]");
                    T.remove(scheuledTask.getTask());
                    scheduledPrint.remove(scheuledTask);
                }

                // 16. to_be_scheduled
                List<Task> to_be_scheduled_tmp = new ArrayList<>();
                for (Task t : adaptedGraph.getVertices()) {
                    if(!(t instanceof Communication) && !to_be_scheduled_tmp.contains(t)) {
                        Collection<Task> parents = GraphUtility.getPredecessorTaskNodes(adaptedGraph, t);
                        boolean allParentsSafe = !parents.isEmpty();
                        for (Task parent : parents) {
                            if(!(!T.containsKey(parent) && T.containsKey(t))) {
                                allParentsSafe = false;
                            }
                        }
                        if(allParentsSafe) {
                            to_be_scheduled_tmp.add(t);
                        }
                    }

                }
                List<Task> to_be_scheduled = new ArrayList<>();
                for (Task t : to_be_scheduled_tmp) {
                    to_be_scheduled.add(0, t);
                }

                // 17. Planandschedule(to_be_scheduled)

                    // 17.1 active_VMs List of active VMs in the VM pool
                    List<VMPoolEntry> active_VMs = new ArrayList<>(VMPoolStatus);

                    // 17.2. For each t_i e task_list do
                    for(Task ti : to_be_scheduled) {

                        boolean exit = false;
                        Resource vp = null;
                        Task lastParent = null;

                        // 17.3 vmmap = CheapesttaskVM(t_i)
                        // 17.3.1. Begin
                        // 17.3.2. taskvmmap = 0
                        taskvmmap = new HashMap<>();
                        // 17.3.3. If t is not an entry task then
                        if(!entryTasks.contains(ti)) {
                            // 17.3.4. lastParent arg(max tp ts parent XFT(tp)
                            Collection<Task> parents = GraphUtility.getPredecessorTaskNodes(adaptedGraph, ti);

                            max = 0.0;
                            for(Task parent : parents) {
                                if(XFT.get(parent) > max) {
                                    max = XFT.get(parent);
                                    lastParent = parent;
                                }
                            }
                            // 17.3.5. vp VM on which lastParent is running
                            vp = scheduledMapping.get(lastParent);

                            // 17.3.6 temp ...
                            max = 0.0;
                            for(Task parent : parents) {
                                if(parent != lastParent && XFT.get(parent) + getTT(parent, ti) > max) {
                                    max = XFT.get(parent) + getTT(parent, ti);
                                }
                            }
                            double temp = Math.max(XFT.get(lastParent), max);

                            // 17.3.7 If temp >= XIST ...
                            double ET = getET(specification, ti, vp);

                            if(temp >= (XST.get(lastParent) + ET) && (temp + XET.get(vp).get(ti)) <= D) {

                                // 17.3.8  XST(t) =  temp
                                XST.replace(ti, temp);

                                // 17.3.9. taskvmmap =  type(vp)
                                taskvmmap.put(ti, vp);

                                // 17.3.10 return taskvmmap
                                exit = true;
                            }
                            // 17.3.11 Else
                            else {

                                // 17.3.12 XST(t) = ...
                                max = 0.0;
                                for(Task parent : parents) {
                                    if(XFT.get(parent) + getTT(ti, parent) > max) {
                                        max = XFT.get(parent) + getTT(ti, parent);
                                    }
                                }
                                if(XST.containsKey(ti)) {
                                    XST.replace(ti, max);
                                }else {
                                    XST.put(ti, max);
                                }

                            } // 17.3.13 End if
                        }
                        // 17.3.14. Else
                        else {
                            // 15. XST = acquisitionDelay
                            XST.put(ti, acquisitionDelay);
                        }

                        if(!exit) {
                            // 17.3.16. End if
                            // 17.3.17. Find {VM_k} e VM_set for which XST(t) + XET(t, VM_k) <= D
                            List<Resource> resources = new ArrayList<>();
                            for (Resource res : XET.keySet()) {
                                double subtract = 0.0;
                                if(vp != null && lastParent != null && vp.getId().equals(res.getId())) {
                                    subtract = getTT(ti, lastParent);
                                }
                                if (XST.get(ti) - subtract + XET.get(res).get(ti) <= D) {
                                    resources.add(res);
                                }
                            }
                            // 17.3.18. VM_j = arg(min_VM_k(XET(t,VM_k)/interval * Cost(VM_K)
                            Resource VM_j = null;
                            double min = Double.MAX_VALUE;
                            for (Resource r : resources) {
                                // TODO check paper there is arg(...)
                                double tmp = XET.get(r).get(ti) / interval * Cost(r);
                                if (min > tmp) {
                                    min = tmp;
                                    VM_j = r;
                                }
                            }
                            // 17.3.19. taskvmmap = VMj
                            taskvmmap.put(ti, VM_j);
                        }

                        // 17.4 Find vk e active VMs
                        List<VMPoolEntry> vk = new ArrayList<>();
                        for(VMPoolEntry entry: active_VMs) {

                            Resource r = entry.getType();
                            calcXFT(ti, specification, adaptedGraph, entry);
                            calcXST(ti, adaptedGraph, entry);
                            calcLFT(ti, adaptedGraph);
                            double CLI = ((int)((entry.getExpecteddIdleStartTime() - entry.getStartTime()) / interval) * interval) + interval;
                            /*if((entry.getExpecteddIdleStartTime() - entry.getStartTime()) % interval != 0) {
                                CLI += interval;
                            }*/

                            if(r.getId().equals(taskvmmap.get(ti).getId())
                                && XST.get(ti) <= CLI
                                && XFT.get(ti) <= LFT.get(ti)) {
                                Collection<Task> children = GraphUtility.getSuccessorTaskNodes(adaptedGraph, ti);
                                boolean safe = true;
                                for(Task child: children) {
                                    double LST = LFT.get(child) - MET.get(child);
                                    calcXFT(child, specification, adaptedGraph, entry);
                                    calcXST(child, adaptedGraph, entry);
                                    if(XST.get(child) > LST) {
                                        safe = false;
                                    }
                                }
                                if(safe) {
                                    vk.add(entry);
                                }
                            }
                        }

                        // 17.5 if vk exists
                        if(!vk.isEmpty()) {

                            // 17.6 Find the VM vk, such that the difference between XIST(vk) and XST(ti) is minimum
                            VMPoolEntry minDiff = null;
                            double min = Double.MAX_VALUE;
                            for(VMPoolEntry v: vk) {
                                double ET = getET(specification, ti, v.getType());
                                double XIST = XST.get(ti) + ET;
                                if(Math.abs(XST.get(ti) - XIST) < min) {
                                    min = Math.abs(XST.get(ti) - XIST);
                                    minDiff = v;
                                }
                            }

                            // 17.7 Schedule ti on vk and update XST(ti)
                            calcXFT(ti, specification, adaptedGraph, minDiff);
                            calcXST(ti, adaptedGraph, minDiff);
                            scheduledMapping.put(ti, minDiff.getType());
                            //System.out.println("Schedule " + ti.getId() + " on " + minDiff.getId());
                            scheduled.add(new Schedule(ti, minDiff.getType(), minDiff.getId(), XST.get(ti), XFT.get(ti)));
                            scheduledPrint.add(new Schedule(ti, minDiff.getType(), minDiff.getId(), XST.get(ti), XFT.get(ti)));
                            //printSchedule();

                            // 17.8 Update VM Pool Status
                            for(VMPoolEntry entry : VMPoolStatus) {
                                if(entry.getId().equals(minDiff.getId())) {
                                    double ET = getET(specification, ti, entry.getType());
                                    calcXST(ti, adaptedGraph, entry);
                                    entry.setExpecteddIdleStartTime(XST.get(ti) + ET);
                                }
                            }
                            //printVMPoolStatus();
                        }
                        // 17.9 Else
                        else {

                            // 17.10 Find(vj) e active_VMs ...
                            List<VMPoolEntry> vj = new ArrayList<>();
                            for(VMPoolEntry entry: active_VMs) {
                                Resource r = entry.getType();
                                calcXFT(ti, specification, adaptedGraph, entry);
                                calcXST(ti, adaptedGraph, entry);
                                calcLFT(ti, adaptedGraph);
                                double CLI = ((int)((entry.getExpecteddIdleStartTime() - entry.getStartTime()) / interval) * interval) + interval;
                                /*if((entry.getExpecteddIdleStartTime() - entry.getStartTime()) % interval != 0) {
                                    CLI += interval;
                                }*/
                                if(r.getId().equals(taskvmmap.get(ti).getId())
                                    && XFT.get(ti) <= CLI
                                    && XFT.get(ti) <= LFT.get(ti)) {
                                    Collection<Task> children = GraphUtility.getSuccessorTaskNodes(adaptedGraph, ti);
                                    boolean safe = true;
                                    for(Task child: children) {
                                        double LST = LFT.get(child) - MET.get(child);
                                        calcXFT(child, specification, adaptedGraph, entry);
                                        calcXST(child, adaptedGraph, entry);
                                        if(XST.get(child) > LST) {
                                            safe = false;
                                        }
                                    }
                                    if(safe) {
                                        vj.add(entry);
                                    }
                                }
                            }

                            // 17.11 If (vj) exists
                            if(!vj.isEmpty()) {

                                // 17.12 Find the VM ...
                                VMPoolEntry minDiff = null;
                                double min = Double.MAX_VALUE;
                                for(VMPoolEntry v: vj) {
                                    double ET = getET(specification, ti, v.getType());
                                    double XIST = XST.get(ti) + ET;
                                    if(Math.abs(XST.get(ti) - XIST) < min) {
                                        min = Math.abs(XST.get(ti) - XIST);
                                        minDiff = v;
                                    }
                                }

                                // 17.13 Schedule ti on vj; update XST(ti)
                                calcXFT(ti, specification, adaptedGraph, minDiff);
                                calcXST(ti, adaptedGraph, minDiff);
                                scheduledMapping.put(ti, minDiff.getType());
                                //System.out.println("Schedule " + ti.getId() + " on " + minDiff.getId());
                                scheduled.add(new Schedule(ti, minDiff.getType(), minDiff.getId(), XST.get(ti), XFT.get(ti)));
                                scheduledPrint.add(new Schedule(ti, minDiff.getType(), minDiff.getId(), XST.get(ti), XFT.get(ti)));
                                //printSchedule();

                                // 17.14 Update VM Pool Status
                                for(VMPoolEntry entry : VMPoolStatus) {
                                    if(entry.getId().equals(minDiff.getId())) {
                                        double ET = getET(specification, ti, entry.getType());
                                        entry.setExpecteddIdleStartTime(XST.get(ti) + ET);
                                    }
                                }
                                //printVMPoolStatus();
                            }
                            // 17.15 Else
                            else {

                                // 17.16 Procure a new VMv of type vmmap from the cloud at XST(ti) - acquistiondelay
                                String instance = "v" + (id++);
                                VMPoolEntry entry_tmp = new VMPoolEntry(instance, taskvmmap.get(ti), null, null, null);
                                VMPoolStatus.add(entry_tmp);
                                //printVMPoolStatus();

                                // 17.17 Schedule ti on v at XST(ti)
                                calcXFT(ti, specification, adaptedGraph, entry_tmp);
                                calcXST(ti, adaptedGraph, entry_tmp);
                                entry_tmp.setStartTime(XST.get(ti) - acquisitionDelay);
                                entry_tmp.setExpecteddIdleStartTime(XST.get(ti) + getET(specification, ti, entry_tmp.getType()));
                                scheduledMapping.put(ti, taskvmmap.get(ti));
                                //System.out.println("Schedule " + ti.getId() + " on " + instance);
                                scheduled.add(new Schedule(ti, taskvmmap.get(ti), instance, XST.get(ti), XFT.get(ti)));
                                scheduledPrint.add(new Schedule(ti, taskvmmap.get(ti), instance, XST.get(ti), XFT.get(ti)));
                                //printSchedule();

                                // 17.18 Update VM Pool Status
                                for(VMPoolEntry entry : VMPoolStatus) {
                                    if(entry.getId().equals(taskvmmap.get(ti).getId())) {
                                        double ET = getET(specification, ti, entry.getType());
                                        entry.setExpecteddIdleStartTime(XST.get(ti) + ET);
                                    }
                                }
                            } // 17.19 End if
                        } // 17.20 End if

                        // Update end time of VM Pool Status
                        for(Task texit: exitTasks) {
                            if(ti.getId().equals(texit.getId())) {
                                for(VMPoolEntry entry : VMPoolStatus) {
                                    Task lastTask = getLastTaskOnInstance(entry.getId());
                                    entry.setEndTime(entry.getExpecteddIdleStartTime() +
                                        getTT(lastTask, ti)
                                    );
                                }
                            }
                        }

                    } // 17.21 End for

                if(!to_be_scheduled.isEmpty()) {
                    // 17.22 Deprovision the idle VMs
                    System.out.println("Deprovision idle VM");

                    //System.out.println("------->");
                    printVMPoolStatus();
                    printSchedule();
                    //System.out.println("<------");
                }

            }

            // 18. End while
            }

        // 19. Else
        else {
            // 20. Prompt user to specify a deadline above MET W
            System.out.println("Specify a deadline above MET W!");
        }
        // 21. End If
        // 22. End

        double totalExecution = 0.0;
        double totalCost = 0.0;

        for(VMPoolEntry entry: VMPoolStatus) {
            double diff = (entry.getEndTime() - entry.getStartTime());
            if(diff % interval != 0) {
                diff = ((int) (diff/interval)) * interval + interval;
            }
            totalCost += diff/interval * Cost(entry.getType());
            if(totalExecution < entry.getEndTime()) {
                totalExecution = entry.getEndTime();
            }
        }

        System.out.println("------------------------------------");
        System.out.println("Total Execution = " + totalExecution);
        System.out.println("Total Cost = $" + totalCost);
        System.out.println("------------------------------------");
    }
}
