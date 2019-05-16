use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;
use std::iter::FromIterator;
use std::sync::mpsc::channel;
use std::sync::Arc;
use std::sync::Mutex;

pub trait Task<D> {
    fn run(&mut self, res: &D);
}

pub struct Dispatcher<T, D> {
    tasks: Vec<Data<'static, T, D>>,
    graph: Graph<T>,
}

impl<T, D> Dispatcher<T, D>
where
    T: Eq + Hash + Clone + Send + Sync + Debug + 'static,
{
    pub fn new() -> Self {
        Dispatcher::default()
    }

    pub fn insert_empty_task<X>(&mut self, id: T, task: X)
    where
        X: Task<D> + Send + 'static,
    {
        self.tasks.push(Data::new(id.clone(), task));
        self.graph.insert_empty_node(id);
    }

    pub fn with_empty_task<X>(mut self, id: T, task: X) -> Self
    where
        X: Task<D> + Send + 'static,
    {
        self.insert_empty_task(id, task);
        self
    }

    pub fn insert_task<X>(&mut self, id: T, task: X, deps: &[T])
    where
        X: Task<D> + Send + 'static,
    {
        self.tasks.push(Data::new(id.clone(), task));
        self.graph.insert_node(id, deps);
    }

    pub fn with_task<X>(mut self, id: T, task: X, deps: &[T]) -> Self
    where
        X: Task<D> + Send + 'static,
    {
        self.insert_task(id, task, deps);
        self
    }

    pub fn insert_inactive_task<X>(&mut self, id: T, task: X, deps: &[T])
    where
        X: Task<D> + Send + 'static,
    {
        self.insert_task(id.clone(), task, deps);
        self.graph.deactivate_node(id);
    }

    pub fn with_inactive_task<X>(mut self, id: T, task: X, deps: &[T]) -> Self
    where
        X: Task<D> + Send + 'static,
    {
        self.insert_inactive_task(id, task, deps);
        self
    }

    pub fn with_dependencies(mut self, id: T, deps: &[T]) -> Self {
        self.graph.insert_dependencies(id, deps);
        self
    }

    pub fn add_dependencies(&mut self, id: T, deps: &[T]) {
        self.graph.insert_dependencies(id, deps);
    }

    pub fn remove_dependencies(&mut self, id: T, deps: &[T]) {
        self.graph.remove_dependencies(id, deps);
    }

    pub fn remove_task(&mut self, id: T) -> Result<(), ()> {
        self.tasks.retain(|d| d.id != id);
        self.graph.remove_node(id.clone())
    }

    pub fn activate_task(&mut self, id: T) {
        self.graph.activate_node(id);
    }

    pub fn deactivate_task(&mut self, id: T) {
        self.graph.deactivate_node(id);
    }

    pub fn dispatch(&mut self, res: &D)
    where
        D: Send + Sync,
    {
        println!("Dispatching graph");
        rayon::scope(|s| {
            let graph = &mut self.graph;
            let tasks = &mut self.tasks;
            graph.start();
            let (tx, rx) = channel();

            while !graph.is_done() {
                let section: Vec<_> = graph.next_section().cloned().collect();
                for id in section {
                    println!("Dispatching task with id {:?}", id);
                    let tx = tx.clone();
                    if let Some(mut data) = tasks.iter().find(|d| d.id == id).cloned() {
                        graph.add_started(id.clone());
                        s.spawn(move |_| {
                            data.run(res);
                            tx.send(id.clone()).unwrap();
                        });
                    }
                }

                let completed_id = rx.recv().unwrap();
                println!("Task with id {:?} completed", completed_id);
                graph.add_completed(completed_id);
            }
        });
        println!("Dispatch done");
    }
}

impl<T, D> Default for Dispatcher<T, D> {
    fn default() -> Self {
        Dispatcher {
            tasks: Vec::default(),
            graph: Graph::default(),
        }
    }
}

struct Graph<T> {
    nodes: Vec<Node<T>>,
    started: Vec<T>,
    completed: Vec<T>,
    dirty: bool,
}

impl<T> Default for Graph<T> {
    fn default() -> Self {
        Graph {
            nodes: Vec::default(),
            started: Vec::default(),
            completed: Vec::default(),
            dirty: false,
        }
    }
}

impl<T> Graph<T>
where
    T: Eq + Hash + Clone,
{
    fn insert_empty_node(&mut self, id: T) {
        let n = Node {
            active: true,
            id,
            deps: HashSet::default(),
        };
        self.nodes.push(n);
        self.dirty = true;
    }

    fn insert_node(&mut self, id: T, deps: &[T]) {
        let n = Node {
            active: true,
            id,
            deps: HashSet::from_iter(deps.iter().cloned()),
        };
        self.nodes.push(n);
        self.dirty = true;
    }

    fn activate_node(&mut self, id: T) {
        if let Some(n) = self.find_node_mut(id) {
            n.active = true;
        }
    }

    fn deactivate_node(&mut self, id: T) {
        if let Some(n) = self.find_node_mut(id) {
            n.active = false;
        }
    }

    fn remove_node(&mut self, id: T) -> Result<(), ()> {
        if self
            .nodes
            .iter()
            .any(|n| n.id != id && n.deps.contains(&id))
        {
            return Err(());
        }
        let pos = self.nodes.iter().position(|n| n.id == id);
        if let Some(p) = pos {
            self.nodes.remove(p);
        }
        Ok(())
    }

    fn insert_dependencies(&mut self, id: T, deps: &[T]) -> Option<()> {
        self.dirty = true;
        self.find_node_mut(id).and_then(|f| {
            f.deps.extend(deps.iter().cloned());
            Some(())
        })
    }

    fn remove_dependencies(&mut self, id: T, deps: &[T]) -> Option<()> {
        self.dirty = true;
        self.find_node_mut(id).and_then(|f| {
            f.deps.retain(|n| !deps.contains(n));
            Some(())
        })
    }

    fn start(&mut self) {
        self.started.clear();
        self.completed.clear();
    }

    fn is_done(&self) -> bool {
        self.completed.len()
            == self
                .nodes
                .iter()
                .filter(|n| n.is_active(&self.nodes))
                .count()
    }

    fn add_completed(&mut self, id: T) {
        self.completed.push(id);
    }

    fn add_started(&mut self, id: T) {
        self.started.push(id);
    }

    fn next_section(&self) -> impl Iterator<Item = &T> + '_ {
        self.nodes
            .iter()
            .filter(move |n| n.is_active(&self.nodes) && !self.started.contains(&n.id))
            .filter(move |n| n.is_ready(&self.completed))
            .map(|n| &n.id)
    }

    fn find_node_mut(&mut self, id: T) -> Option<&mut Node<T>> {
        self.nodes.iter_mut().find(|f| f.id == id)
    }
}

struct Node<T> {
    id: T,
    active: bool,
    deps: HashSet<T>,
}

impl<T> Node<T> {
    fn is_ready(&self, completed: &Vec<T>) -> bool
    where
        T: PartialEq,
    {
        self.deps.is_empty() || !self.deps.iter().any(|d| !completed.contains(d))
    }

    fn is_active(&self, nodes: &Vec<Node<T>>) -> bool
    where
        T: PartialEq,
    {
        self.active
            && self.deps.iter().all(|d| {
                nodes.iter().find(|n| n.id == *d).is_some()
                    && nodes.iter().any(|n| n.id == *d && n.is_active(nodes))
            })
    }
}

struct Data<'a, T, D> {
    task: Arc<Mutex<Box<Task<D> + Send + 'a>>>,
    id: T,
}

impl<'a, T, D> Clone for Data<'a, T, D>
where
    T: Clone,
{
    fn clone(&self) -> Self {
        Data {
            task: self.task.clone(),
            id: self.id.clone(),
        }
    }
}

impl<'a, T, D> Data<'a, T, D> {
    fn new<X>(id: T, task: X) -> Self
    where
        X: Task<D> + Send + 'a,
    {
        Data {
            id,
            task: Arc::new(Mutex::new(Box::new(task))),
        }
    }

    fn run(&mut self, res: &D) {
        let mut task = self.task.lock().unwrap();
        task.run(res);
    }
}

#[cfg(test)]
mod tests {
    use crate::Dispatcher;
    use crate::Task;

    pub struct Resources;

    pub struct Task12;

    impl Task<Resources> for Task12 {
        fn run(&mut self, _res: &Resources) {
            println!("Running Task12");
        }
    }

    pub struct Task13;

    impl Task<Resources> for Task13 {
        fn run(&mut self, _res: &Resources) {
            println!("Running Task13");
        }
    }

    pub struct Task14;

    impl Task<Resources> for Task14 {
        fn run(&mut self, _res: &Resources) {
            println!("Running Task14");
        }
    }

    pub struct Task34;

    impl Task<Resources> for Task34 {
        fn run(&mut self, _res: &Resources) {
            println!("Running Task34");
        }
    }

    pub struct Task66;

    impl Task<Resources> for Task66 {
        fn run(&mut self, _res: &Resources) {
            println!("Running Task66");
        }
    }

    pub struct Task77;

    impl Task<Resources> for Task77 {
        fn run(&mut self, _res: &Resources) {
            println!("Running Task77");
        }
    }

    pub struct Task78;

    impl Task<Resources> for Task78 {
        fn run(&mut self, _res: &Resources) {
            println!("Running Task78");
        }
    }

    #[test]
    fn dispatch() {
        let mut dispatcher = Dispatcher::new()
            .with_empty_task(0, Task12)
            .with_empty_task(1, Task13)
            .with_task(2, Task14, &[3])
            .with_task(3, Task34, &[0])
            .with_task(4, Task66, &[3, 0])
            .with_inactive_task(5, Task77, &[0])
            .with_task(6, Task78, &[5]);
        let res = Resources {};
        dispatcher.dispatch(&res);
        dispatcher.activate_task(5);
        dispatcher.dispatch(&res);
        dispatcher.deactivate_task(0);
        dispatcher.dispatch(&res);
        dispatcher.remove_dependencies(6, &[5]);
        dispatcher.remove_task(5).unwrap();
        dispatcher.activate_task(0);
        dispatcher.dispatch(&res);
    }
}
