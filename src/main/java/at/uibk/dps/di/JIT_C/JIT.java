package at.uibk.dps.di.JIT_C;

import at.uibk.dps.di.properties.PropertyServiceScheduler;
import at.uibk.dps.di.schedulerV2.GraphUtility;
import at.uibk.dps.ee.model.graph.EnactmentGraph;
import at.uibk.dps.ee.model.graph.EnactmentSpecification;
import at.uibk.dps.ee.model.properties.PropertyServiceData;
import at.uibk.dps.ee.model.properties.PropertyServiceDependency;
import at.uibk.dps.ee.model.properties.PropertyServiceFunctionUser;
import com.google.gson.JsonPrimitive;
import net.sf.opendse.model.Communication;
import net.sf.opendse.model.Mapping;
import net.sf.opendse.model.Resource;
import net.sf.opendse.model.Task;

import java.util.*;

public class JIT {

    private double[][] getTransferTime(){
        double[][] TT =
            {
                {0,6,6,6,0,0,0,0,0},
                {0,0,0,0,4,4,0,0,0},
                {0,0,0,0,0,5,0,0,0},
                {0,0,0,0,0,0,4,0,0},
                {0,0,0,0,0,0,0,3,0},
                {0,0,0,0,0,0,0,2,0},
                {0,0,0,0,0,0,0,4,0},
                {0,0,0,0,0,0,0,0,8},
                {0,0,0,0,0,0,0,0,0}
            };
        return TT;
    }
    private static final double acquisitiondelay = 1.0;
    private static final double interval = 10.0;

    private Double Cost(Resource resource){
        if(resource.getId().equals("Vl")) {
            return 0.04;
        } else if(resource.getId().equals("Vm")) {
            return 0.02;
        } else if(resource.getId().equals("Vs")) {
            return 0.01;
        }
        return null;
    }


    private int id = 1;
    private int iteration = 1;
    Map<Task, Resource> scheduledMapping = new HashMap<>();
    Map<Task, Double> AST = new HashMap<>();
    Map<Task, Double> XFT = new HashMap<>();
    Map<Task, Double> XST = new HashMap<>();
    Map<Task, Double> LFT = new HashMap<>();
    Map<Task, Double> MET = new HashMap<>();

    private List<Schedule> scheduled = new ArrayList<>();
    private List<VMPoolEntry> VMPoolStatus = new ArrayList<>();

    private Map<String, Double> vmstarttimes = new HashMap<>();

    private void printVMPoolStatus() {
        for(VMPoolEntry entry: VMPoolStatus) {
            /*System.out.println("VMPOOLSTATUS -- id: " + entry.getId() + ", type: " + entry.getType().getId() + ", startTime: " + entry.getStartTime() +
                ", ExpectedIdleStartTime: " + entry.getExpecteddIdleStartTime() + ", endTime: " + entry.getEndTime());*/
            System.out.println("VMPOOLSTATUS:\t " + entry.getId() + " | " + entry.getType().getId() + " | " + entry.getStartTime() +
                " | " + entry.getExpecteddIdleStartTime() + " | " + entry.getEndTime());
        }
    }

    private void printSchedule() {
        for(Schedule entry: scheduled) {
            /*System.out.println("SCHEDULE -- id: " + entry.getTask() + ", VM: " + entry.getVm() + " (" + entry.getInstance() + "), XST: " + entry.getXst() +
                ", XFT: " + entry.getXft());*/
            System.out.println("SCHEDULE:\t\t" + entry.getTask() + " | " + entry.getVm() + " (" + entry.getInstance() + ") | " + entry.getXst() +
                " | " + entry.getXft());
        }
    }

    private double getTT(Task t1, Task t2){
        double[][] TT = getTransferTime();
        double tt = 0.0;
        if(t1.getId().contains("+")) {
            String[] tasks = t1.getId().split("\\+");
            for(String task: tasks) {
                if(t2.getId().contains("+")) {
                    String[] tsks = t2.getId().split("\\+");
                    for(String ta: tsks) {
                        tt += TT[Integer.valueOf(
                            ta.replaceAll("[^0-9]", "")) - 1][
                            Integer.valueOf(task.replaceAll("[^0-9]", "")) - 1];
                    }
                }else {
                    tt += TT[Integer.valueOf(
                        t2.getId().replaceAll("[^0-9]", "")) - 1][
                        Integer.valueOf(task.replaceAll("[^0-9]", "")) - 1];
                }
            }
        }else if(t2.getId().contains("+")) {
            String[] tasks = t2.getId().split("\\+");
            for(String task: tasks) {
                if(t1.getId().contains("+")) {
                    String[] tsks = t1.getId().split("\\+");
                    for(String ta: tsks) {
                        tt += TT[Integer.valueOf(
                            ta.replaceAll("[^0-9]", "")) - 1][
                            Integer.valueOf(task.replaceAll("[^0-9]", "")) - 1];
                    }
                }else {
                    tt += TT[Integer.valueOf(
                        t1.getId().replaceAll("[^0-9]", "")) - 1][
                        Integer.valueOf(task.replaceAll("[^0-9]", "")) - 1];
                }
            }
        } else {
            tt = TT[Integer.valueOf(t2.getId().replaceAll("[^0-9]", "")) - 1]
                    [Integer.valueOf(t1.getId().replaceAll("[^0-9]", "")) - 1];
        }

        return tt;
    }

    public void schedule(EnactmentSpecification specification, double D){

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
                        double dur = PropertyServiceScheduler.getDuration(map);
                        if(dur < min) {
                            min = dur;
                        }
                    }
                    MET_tmp.put(t, min);
                }
            }

            // Eq. 3: Earliest start time of task t_i
            Map<Task, Double> EST = new HashMap<>();
            double[][] TT = getTransferTime();
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
                            double tmp = EST.get(predecessor) + MET_tmp.get(predecessor) + TT[
                                Integer.valueOf(predecessor.getId().replaceAll("[^0-9]", "")) - 1][
                                Integer.valueOf(successor.getId().replaceAll("[^0-9]", "")) - 1];
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
            EnactmentGraph adaptedGraph = preprocessing();

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
                                if(successor.getId().contains("+")) {
                                    String[] tasks = successor.getId().split("\\+");
                                    double tt = 0.0;
                                    for(String task: tasks) {
                                        if(predecessor.getId().contains("+")) {
                                            String[] tsks = predecessor.getId().split("\\+");
                                            for(String ta: tsks) {
                                                tt += TT[Integer.valueOf(
                                                    ta.replaceAll("[^0-9]", "")) - 1][
                                                    Integer.valueOf(task.replaceAll("[^0-9]", "")) - 1];
                                            }
                                        }else {
                                            tt += TT[Integer.valueOf(
                                                predecessor.getId().replaceAll("[^0-9]", "")) - 1][
                                                Integer.valueOf(task.replaceAll("[^0-9]", "")) - 1];
                                        }
                                    }
                                    double tmp = LFT.get(successor) - MET.get(successor) - tt;
                                    if (tmp < min) {
                                        min = tmp;
                                    }
                                }else if(predecessor.getId().contains("+")) {
                                    String[] tasks = predecessor.getId().split("\\+");
                                    double tt = 0.0;
                                    for(String task: tasks) {
                                        if(successor.getId().contains("+")) {
                                            String[] tsks = successor.getId().split("\\+");
                                            for(String ta: tsks) {
                                                tt += TT[Integer.valueOf(
                                                    ta.replaceAll("[^0-9]", "")) - 1][
                                                    Integer.valueOf(task.replaceAll("[^0-9]", "")) - 1];
                                            }
                                        }else {
                                            tt += TT[Integer.valueOf(
                                                successor.getId().replaceAll("[^0-9]", "")) - 1][
                                                Integer.valueOf(task.replaceAll("[^0-9]", "")) - 1];
                                        }
                                    }
                                    double tmp = LFT.get(successor) - MET.get(successor) - tt;
                                    if (tmp < min) {
                                        min = tmp;
                                    }
                                } else {
                                    double tmp = LFT.get(successor) - MET.get(successor) -
                                        TT[Integer.valueOf(predecessor.getId().replaceAll("[^0-9]", "")) - 1]
                                            [Integer.valueOf(successor.getId().replaceAll("[^0-9]", "")) - 1];
                                    if (tmp < min) {
                                        min = tmp;
                                    }
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
            Map<Task, Resource> taskvmmap = null;


            // 7. For each te 2 {t_entry}
            for(Task t_e : entryTasks) {
                // 8. to_provision CheapesttaskVMMap(t_e)
                    // 8.1. Begin
                    // 8.2. taskvmmap = 0
                    taskvmmap = new HashMap<>();
                    // 8.3. If t is not an entry task then
                    if(!entryTasks.contains(t_e)) {
                        // TODO
                    }
                    // 8.14. Else
                    else {
                        // 15. XST = acquisitiondelay
                        XST.put(t_e, acquisitiondelay);
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
                vmstarttimes.put(instance, (double) System.currentTimeMillis());

                // 10. Schedule t_e on v_e at XST(t_e)
                // LATER

                // 11. Update VM Pool Status
                Set<Mapping<Task, Resource>> mappings = specification.getMappings().getMappings(t_e);
                double ET = 0.0;
                for(Mapping<Task, Resource> map: mappings) {
                    if(map.getTarget().getId().equals(to_provision.get(t_e).getId())) {
                        ET = PropertyServiceScheduler.getDuration(map);
                    }
                }
                VMPoolEntry e = new VMPoolEntry(instance, to_provision.get(t_e), 0.0, XST.get(t_e) + ET, null);
                VMPoolStatus.add(e);
                //printVMPoolStatus();

                // 10. Schedule t_e on v_e at XST(t_e)
                calcXFT(t_e, specification, adaptedGraph, to_provision.get(t_e), e);
                XFT.replace(t_e, XFT.get(t_e) + XST.get(t_e));
                scheduled.add(new Schedule(t_e, to_provision.get(t_e), instance, XST.get(t_e), XFT.get(t_e)));
                //printSchedule();

            } // 12. End for



            // 13. While all tasks in T are not completed do
            Map<Task, Boolean> completed = new HashMap<>();
            for(Task t: adaptedGraph.getVertices()) {
                if(!(t instanceof Communication)) {
                    completed.put(t, false);
                }
            }

            //System.out.println("------->");
            printVMPoolStatus();
            printSchedule();
            //System.out.println("<------");

            while(!completed.isEmpty()){

                // 14. Send the scheduled tasks for execution to the execution manager
                while (!scheduled.isEmpty()) {

                    System.out.println("---------------- iteration " + (iteration++) + " ----------------");

                    Schedule scheuledTask = scheduled.get(0);
                    System.out.println("\tSend scheduled task " + scheuledTask.getTask().getId() + " to execution manager.");
                    scheduledMapping.put(scheuledTask.getTask(), scheuledTask.getVm());

                    // 15. Update AST (Actual start time), XFT of scheduled tasks
                    AST.put(scheuledTask.getTask(), XST.get(scheuledTask.getTask()));
                    Set<Mapping<Task, Resource>> mappings = specification.getMappings().getMappings(scheuledTask.getTask());
                    double ET = 0.0;
                    for(Mapping<Task, Resource> map: mappings) {
                        if(map.getTarget().getId().equals(scheuledTask.getVm().getId())) {
                            ET = PropertyServiceScheduler.getDuration(map);
                        }
                    }
                    XFT.put(scheuledTask.getTask(), AST.get(scheuledTask.getTask()) + ET);
                    scheuledTask.setXst(XST.get(scheuledTask.getTask()));
                    scheuledTask.setXft(XFT.get(scheuledTask.getTask()));

                    // 16. to_be_scheduled
                    List<Task> to_be_scheduled_tmp = new ArrayList<>();
                    for(Task t: adaptedGraph.getVertices()) {
                        Collection<Task> parents =
                            GraphUtility.getPredecessorTaskNodes(adaptedGraph, t);
                        for (Task parent : parents) {
                            for (Schedule sched : scheduled) {
                                if (sched.getTask().getId().equals(parent.getId())) {
                                    if (!(t instanceof Communication)) {
                                        to_be_scheduled_tmp.add(t);
                                    }
                                }
                            }
                        }
                    }
                    List<Task> to_be_scheduled = new ArrayList<>();
                    for(Task t: to_be_scheduled_tmp) {
                        to_be_scheduled.add(0, t);
                    }

                    boolean exit = false;


                    // 17. Planandschedule(to_be_scheduled)

                        // 17.1 active_VMs List of active VMs in the VM pool
                        List<VMPoolEntry> active_VMs = new ArrayList<>();
                        for(VMPoolEntry entry : VMPoolStatus) {
                            active_VMs.add(entry);
                        }

                        // 17.2. For each t_i e task_list do
                        for(Task ti : to_be_scheduled) {

                            // 17.3 vmmap = CheapesttaskVM(t_i)
                            // 17.3.1. Begin
                            // 17.3.2. taskvmmap = 0
                            taskvmmap = new HashMap<>();
                            // 17.3.3. If t is not an entry task then
                            if(!entryTasks.contains(ti)) {
                                // 17.3.4. lastParent arg(max tp ts parent XFT(tp)
                                Collection<Task> parents = GraphUtility.getPredecessorTaskNodes(adaptedGraph, ti);
                                Task lastParent = null;
                                max = 0.0;
                                for(Task parent : parents) {
                                    if(XFT.get(parent) > max) {
                                        max = XFT.get(parent);
                                        lastParent = parent;
                                    }
                                }
                                // 17.3.5. vp VM on which lastParent is running
                                Resource vp = scheduledMapping.get(lastParent);

                                // 17.3.6 temp ...
                                max = 0.0;
                                for(Task parent : parents) {
                                    if(parent != lastParent && XFT.get(parent) + getTT(parent, ti) > max) {
                                        max = XFT.get(parent) + getTT(parent, ti);
                                    }
                                }
                                double temp = Math.max(XFT.get(lastParent), max);

                                // 17.3.7 If temp >= XIST ...
                                mappings = specification.getMappings().getMappings(scheuledTask.getTask());
                                ET = 0.0;
                                for(Mapping<Task, Resource> map: mappings) {
                                    if(map.getTarget().getId().equals(vp.getId())) {
                                        ET = PropertyServiceScheduler.getDuration(map);
                                    }
                                }
                                if(temp >= (XST.get(scheuledTask.getTask()) + ET) && (temp + XET.get(vp).get(ti)) <= D) {

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
                                        if(XFT.get(parent) + getTT(parent, ti) > max) {
                                            max = XFT.get(parent) + getTT(parent, ti);
                                        }
                                    }
                                    XST.replace(ti, max);

                                } // 17.3.13 End if
                            }
                            // 17.3.14. Else
                            else {
                                // 15. XST = acquisitiondelay
                                XST.put(ti, acquisitiondelay);
                            }

                            if(!exit) {
                                // 17.3.16. End if
                                // 17.3.17. Find {VM_k} e VM_set for which XST(t) + XET(t, VM_k) <= D
                                List<Resource> resources = new ArrayList<>();
                                for(Resource res : XET.keySet()) {
                                    calcXST(ti, adaptedGraph, null);
                                    if(XST.get(ti) + XET.get(res).get(ti) <= D) {
                                        resources.add(res);
                                    }
                                }
                                // 17.3.18. VM_j = arg(min_VM_k(XET(t,VM_k)/interval * Cost(VM_K)
                                Resource VM_j = null;
                                double min = Double.MAX_VALUE;
                                for(Resource r: resources) {
                                    // TODO check paper there is arg(...)
                                    double tmp = XET.get(r).get(ti)/interval * Cost(r);
                                    if(min > tmp){
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
                                calcXFT(ti, specification, adaptedGraph, r, entry);
                                calcXST(ti, adaptedGraph, entry);
                                calcLFT(ti, adaptedGraph);
                                double CLI = ((int)((entry.getExpecteddIdleStartTime() - entry.getStartTime()) / interval) * interval);
                                if((entry.getExpecteddIdleStartTime() - entry.getStartTime()) % interval != 0) {
                                    CLI += interval;
                                }
                                if(r.getId().equals(taskvmmap.get(ti).getId())
                                    && XST.get(ti) <= CLI
                                    && XFT.get(ti) <= LFT.get(ti)) {
                                    Collection<Task> children = GraphUtility.getSuccessorTaskNodes(adaptedGraph, ti);
                                    boolean safe = true;
                                    for(Task child: children) {
                                        double LST = LFT.get(child) - MET.get(child);
                                        calcXFT(child, specification, adaptedGraph, r, entry);
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
                                    ET = 0.0;
                                    mappings = specification.getMappings().getMappings(ti);
                                    for(Mapping<Task, Resource> map: mappings) {
                                        if(map.getTarget().getId().equals(v.getId())) {
                                            ET = PropertyServiceScheduler.getDuration(map);
                                        }
                                    }
                                    double XIST = XST.get(ti) + ET;
                                    if(Math.abs(XST.get(ti) - XIST) < min) {
                                        min = Math.abs(XST.get(ti) - XIST);
                                        minDiff = v;
                                    }
                                }

                                // 17.7 Schedule ti on vk and update XST(ti)
                                calcXST(ti, adaptedGraph, minDiff);
                                scheduledMapping.put(ti, minDiff.getType());
                                //System.out.println("Schedule " + ti.getId() + " on " + minDiff.getId());
                                scheduled.add(new Schedule(ti, minDiff.getType(), minDiff.getId(), XST.get(ti), XFT.get(ti)));
                                //printSchedule();

                                // 17.8 Update VM Pool Status
                                for(VMPoolEntry entry : VMPoolStatus) {
                                    if(entry.getId().equals(minDiff.getId())) {
                                        ET = 0.0;
                                        mappings = specification.getMappings().getMappings(ti);
                                        for(Mapping<Task, Resource> map: mappings) {
                                            if(map.getTarget().getId().equals(entry.getType().getId())) {
                                                ET = PropertyServiceScheduler.getDuration(map);
                                            }
                                        }
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
                                    calcXFT(ti, specification, adaptedGraph, r, entry);
                                    calcXST(ti, adaptedGraph, entry);
                                    calcLFT(ti, adaptedGraph);
                                    double CLI = ((int)((entry.getExpecteddIdleStartTime() - entry.getStartTime()) / interval) * interval);
                                    if((entry.getExpecteddIdleStartTime() - entry.getStartTime()) % interval != 0) {
                                        CLI += interval;
                                    }
                                    if(r.getId().equals(taskvmmap.get(ti).getId())
                                        && XFT.get(ti) <= CLI
                                        && XFT.get(ti) <= LFT.get(ti)) {
                                        Collection<Task> children = GraphUtility.getSuccessorTaskNodes(adaptedGraph, ti);
                                        boolean safe = true;
                                        for(Task child: children) {
                                            double LST = LFT.get(child) - MET.get(child);
                                            calcXFT(child, specification, adaptedGraph, r, entry);
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
                                        ET = 0.0;
                                        mappings = specification.getMappings().getMappings(ti);
                                        for(Mapping<Task, Resource> map: mappings) {
                                            if(map.getTarget().getId().equals(v.getId())) {
                                                ET = PropertyServiceScheduler.getDuration(map);
                                            }
                                        }
                                        double XIST = XST.get(ti) + ET;
                                        if(Math.abs(XST.get(ti) - XIST) < min) {
                                            min = Math.abs(XST.get(ti) - XIST);
                                            minDiff = v;
                                        }
                                    }

                                    // 17.13 Schedule ti on vj; update XST(ti)
                                    calcXST(ti, adaptedGraph, minDiff);
                                    scheduledMapping.put(ti, minDiff.getType());
                                    //System.out.println("Schedule " + ti.getId() + " on " + minDiff.getId());
                                    scheduled.add(new Schedule(ti, minDiff.getType(), minDiff.getId(), XST.get(ti), XFT.get(ti)));
                                    //printSchedule();

                                    // 17.14 Update VM Pool Status
                                    for(VMPoolEntry entry : VMPoolStatus) {
                                        if(entry.getId().equals(minDiff.getId())) {
                                            ET = 0.0;
                                            mappings = specification.getMappings().getMappings(ti);
                                            for(Mapping<Task, Resource> map: mappings) {
                                                if(map.getTarget().getId().equals(entry.getType().getId())) {
                                                    ET = PropertyServiceScheduler.getDuration(map);
                                                }
                                            }
                                            entry.setExpecteddIdleStartTime(XST.get(ti) + ET);
                                        }
                                    }
                                    //printVMPoolStatus();
                                }
                                // 17.15 Else
                                else {

                                    // 17.16 Procure a new VMv of type vmmap from the cloud at XST(ti) - acquistiondelay
                                    ET = 0.0;
                                    for(Mapping<Task, Resource> map: mappings) {
                                        if(map.getTarget().getId().equals(taskvmmap.get(ti).getId())) {
                                            ET = PropertyServiceScheduler.getDuration(map);
                                        }
                                    }
                                    String instance = "v" + (id++);
                                    VMPoolEntry entry_tmp = new VMPoolEntry(instance, taskvmmap.get(ti), null, null, null);
                                    VMPoolStatus.add(entry_tmp);
                                    //printVMPoolStatus();

                                    // 17.17 Schedule ti on v at XST(ti)
                                    calcXFT(ti, specification, adaptedGraph, entry_tmp.getType(), entry_tmp);
                                    calcXST(ti, adaptedGraph, entry_tmp);
                                    entry_tmp.setStartTime(XST.get(ti) - acquisitiondelay);
                                    entry_tmp.setExpecteddIdleStartTime(XST.get(ti) + getET(specification, ti, entry_tmp.getType()));
                                    scheduledMapping.put(ti, taskvmmap.get(ti));
                                    //System.out.println("Schedule " + ti.getId() + " on " + instance);
                                    scheduled.add(new Schedule(ti, taskvmmap.get(ti), instance, XST.get(ti), XFT.get(ti)));
                                    //printSchedule();

                                    // 17.18 Update VM Pool Status
                                    for(VMPoolEntry entry : VMPoolStatus) {
                                        if(entry.getId().equals(taskvmmap.get(ti).getId())) {
                                            ET = 0.0;
                                            mappings = specification.getMappings().getMappings(ti);
                                            for(Mapping<Task, Resource> map: mappings) {
                                                if(map.getTarget().getId().equals(entry.getType().getId())) {
                                                    ET = PropertyServiceScheduler.getDuration(map);
                                                }
                                            }
                                            entry.setExpecteddIdleStartTime(XST.get(ti) + ET);
                                        }
                                    }
                                    //printVMPoolStatus();
                                } // 17.19 End if
                            } // 17.20 End if
                        } // 17.21 End for

                    // 17.22 Deprovision the idle VMs
                    System.out.println("Deprovision idle VM");

                    System.out.println("[[[ SIMULATED: " + scheuledTask.getTask().getId() + " finished ]]]");
                    completed.remove(scheuledTask.getTask());
                    scheduled.remove(scheuledTask);

                    //System.out.println("------->");
                    printVMPoolStatus();
                    printSchedule();
                    //System.out.println("<------");
                }

            // 18. End while
            }
        }

        // 19. Else
        else {
            // 20. Prompt user to specify a deadline above MET W
            System.out.println("Specify a deadline above MET W!");
        }
        // 21. End If
        // 22. End
    }

    private void calcXST(Task t, EnactmentGraph eGraph, VMPoolEntry entry) {
        double max = 0.0;
        Collection<Task> parents = GraphUtility.getPredecessorTaskNodes(eGraph, t);
        for(Task parent: parents) {
            double delay = getTT(t, parent);
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

    private void calcXFT(Task t, EnactmentSpecification specification, EnactmentGraph eGraph, Resource r, VMPoolEntry entry) {
        Collection<Task> parents = GraphUtility.getPredecessorTaskNodes(eGraph, t);
        double max = 0.0;
        for(Task parent: parents) {
            if(!XFT.containsKey(parent)) {
                calcXFT(parent, specification, eGraph, r, entry);
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

    double getET(EnactmentSpecification specification, Task t, Resource r){
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


    private EnactmentGraph preprocessing() {
        // TODO I think we do not need to implement this
        final Task comm1 = new Communication("commNode1");
        final Task comm2 = new Communication("commNode2");
        final Task comm3 = new Communication("commNode3");
        final Task comm4 = new Communication("commNode4");
        final Task comm5 = new Communication("commNode5");
        final Task comm6 = new Communication("commNode6");
        final Task comm7 = new Communication("commNode7");
        final Task comm8 = new Communication("commNode8");
        final Task comm10 = new Communication("commNode10");
        final Task task1 = PropertyServiceFunctionUser.createUserTask("taskNode1", "noop");
        final Task task2 = PropertyServiceFunctionUser.createUserTask("taskNode2", "noop");
        final Task task3 = PropertyServiceFunctionUser.createUserTask("taskNode3", "noop");
        final Task task4u7 = PropertyServiceFunctionUser.createUserTask("taskNode4+7", "noop");
        final Task task5 = PropertyServiceFunctionUser.createUserTask("taskNode5", "noop");
        final Task task6 = PropertyServiceFunctionUser.createUserTask("taskNode6", "noop");
        final Task task8u9 = PropertyServiceFunctionUser.createUserTask("taskNode8+9", "noop");
        EnactmentGraph graph = new EnactmentGraph();
        PropertyServiceData.setContent(comm1, new JsonPrimitive(true));
        PropertyServiceDependency.addDataDependency(comm1, task1, "key1", graph);

        PropertyServiceDependency.addDataDependency(task1, comm2, "key1", graph);

        PropertyServiceDependency.addDataDependency(comm2, task2, "key2", graph);
        PropertyServiceDependency.addDataDependency(comm2, task3, "key2", graph);
        PropertyServiceDependency.addDataDependency(comm2, task4u7, "key2", graph);

        PropertyServiceDependency.addDataDependency(task2, comm3, "key2", graph);
        PropertyServiceDependency.addDataDependency(task2, comm4, "key2", graph);
        PropertyServiceDependency.addDataDependency(task4u7, comm6, "key2", graph);
        PropertyServiceDependency.addDataDependency(task3, comm5, "key2", graph);

        PropertyServiceDependency.addDataDependency(comm3, task5, "key2", graph);
        PropertyServiceDependency.addDataDependency(comm4, task6, "key2", graph);
        PropertyServiceDependency.addDataDependency(comm5, task6, "key2", graph);

        PropertyServiceDependency.addDataDependency(task5, comm7, "key2", graph);
        PropertyServiceDependency.addDataDependency(task6, comm8, "key2", graph);

        PropertyServiceDependency.addDataDependency(comm7, task8u9, "key2", graph);
        PropertyServiceDependency.addDataDependency(comm8, task8u9, "key2", graph);
        PropertyServiceDependency.addDataDependency(comm6, task8u9, "key2", graph);

        PropertyServiceDependency.addDataDependency(task8u9, comm10, "key2", graph);

        PropertyServiceData.makeRoot(comm1);
        PropertyServiceData.makeLeaf(comm10);
        return graph;
    }
}
