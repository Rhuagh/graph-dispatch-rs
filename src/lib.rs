use std::collections::HashSet;
use std::hash::Hash;
use std::iter::FromIterator;

pub struct Graph<T> {
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
    pub fn new() -> Self {
        Graph::default()
    }

    pub fn insert_empty_node(&mut self, id: T) {
        let n = Node {
            active: true,
            id,
            deps: HashSet::default(),
        };
        self.nodes.push(n);
        self.dirty = true;
    }

    pub fn insert_node(&mut self, id: T, deps: &[T]) {
        let n = Node {
            active: true,
            id,
            deps: HashSet::from_iter(deps.iter().cloned()),
        };
        self.nodes.push(n);
        self.dirty = true;
    }

    pub fn with_empty_node(mut self, id: T) -> Self {
        self.insert_empty_node(id);
        self
    }

    pub fn with_node(mut self, id: T, deps: &[T]) -> Self {
        self.insert_node(id, deps);
        self
    }

    pub fn with_inactive_node(mut self, id: T, deps: &[T]) -> Self {
        self.insert_node(id.clone(), deps);
        if let Some(n) = self.find_node_mut(id) {
            n.active = false;
        }
        self
    }

    pub fn activate_node(&mut self, id: T) {
        if let Some(n) = self.find_node_mut(id) {
            n.active = true;
        }
    }

    pub fn deactivate_node(&mut self, id: T) {
        if let Some(n) = self.find_node_mut(id) {
            n.active = false;
        }
    }

    pub fn with_active_node(mut self, id: T) -> Self {
        self.activate_node(id);
        self
    }

    pub fn with_active_nodes(mut self, ids: &[T]) -> Self {
        for id in ids {
            self.activate_node(id.clone());
        }
        self
    }

    pub fn remove_node(&mut self, id: T) -> Result<(), ()> {
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

    pub fn insert_dependency(&mut self, id: T, dep_id: T) -> Option<()> {
        self.dirty = true;
        self.find_node_mut(id).and_then(|f| {
            f.deps.insert(dep_id);
            Some(())
        })
    }

    pub fn remove_dependency(&mut self, id: T, dep_id: T) -> Option<()> {
        self.dirty = true;
        self.find_node_mut(id).and_then(|f| {
            f.deps.remove(&dep_id);
            Some(())
        })
    }

    pub fn with_dependency(mut self, id: T, dep_id: T) -> Self {
        self.insert_dependency(id, dep_id);
        self
    }

    pub fn insert_dependencies(&mut self, id: T, deps: &[T]) -> Option<()> {
        self.dirty = true;
        self.find_node_mut(id).and_then(|f| {
            f.deps.extend(deps.iter().cloned());
            Some(())
        })
    }

    pub fn remove_dependencies(&mut self, id: T, deps: &[T]) -> Option<()> {
        self.dirty = true;
        self.find_node_mut(id).and_then(|f| {
            f.deps.retain(|n| !deps.contains(n));
            Some(())
        })
    }

    pub fn with_dependencies(mut self, id: T, deps: &[T]) -> Self {
        self.insert_dependencies(id, deps);
        self
    }

    pub fn start(&mut self) {
        self.started.clear();
        self.completed.clear();
    }

    pub fn is_done(&self) -> bool {
        self.completed.len()
            == self
                .nodes
                .iter()
                .filter(|n| n.is_active(&self.nodes))
                .count()
    }

    pub fn add_completed(&mut self, id: T) {
        self.completed.push(id);
    }

    pub fn add_started(&mut self, id: T) {
        self.started.push(id);
    }

    pub fn next(&self) -> Option<T> {
        self.nodes
            .iter()
            .filter(move |n| n.is_active(&self.nodes) && !self.started.contains(&n.id))
            .find(|n| n.is_ready(&self.completed))
            .map(|n| n.id.clone())
    }

    pub fn next_section(&self) -> impl Iterator<Item = &T> + '_ {
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
        self.active && self.deps.iter().all(|d| nodes.iter().any(|n| n.id == *d && n.is_active(nodes)))
    }
}

#[cfg(test)]
mod tests {
    use crate::Graph;
    use std::sync::mpsc::channel;

    #[test]
    fn dispatch() {
        let mut graph = Graph::<usize>::default()
            .with_empty_node(12)
            .with_empty_node(13)
            .with_node(14, &[34])
            .with_node(34, &[12])
            .with_node(66, &[34, 12])
            .with_inactive_node(77, &[12])
            .with_node(78, &[77]); // should not be run since a dependency is inactive

        graph.start();
        let (tx, rx) = channel();
        while !graph.is_done() {
            let section : Vec<_> = graph.next_section().cloned().collect();
            for id in section {
                println!("Dispatching: {}", id);
                let tx = tx.clone();
                rayon::spawn(move || {
                    println!("Running task for {}", id);
                    tx.send(id.clone()).unwrap();
                });
                graph.add_started(id);
            }
            let completed_id = rx.recv().unwrap();
            println!("Completed: {}", completed_id);
            graph.add_completed(completed_id);
        }
    }
}
