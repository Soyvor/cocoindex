use std::sync::{Arc, Mutex};
use std::collections::BTreeMap;
use pyo3::{prelude::*, exceptions::PyException};

#[pyclass]
#[derive(Debug, Clone)]
pub struct TaskScope {
    pub name: String,
    pub parent: Option<Arc<TaskScope>>,
}

impl TaskScope {
    pub fn new(name: String, parent: Option<Arc<TaskScope>>) -> Arc<Self> {
        Arc::new(Self { name, parent })
    }
}

#[pyclass]
#[derive(Debug, Clone)]
pub struct TaskStep {
    name: String,
    scope: Arc<TaskScope>,
}



#[pymethods]
impl TaskStep {
    #[new]
    fn new(name: String, scope: TaskScopeRef) -> Self {
        TaskStep { name, scope: scope.0.clone() }
    }

    pub fn __str__(&self) -> String {
        format!("TaskStep(name={}, scope={})", self.name, self.scope.name)
    }
}

#[pyclass]
#[derive(Debug, Clone)]
pub struct TaskScopeRef(Arc<TaskScope>);

#[pymethods]
impl TaskScopeRef {
    pub fn __str__(&self) -> String {
        format!("TaskScope({})", self.0.name)
    }
}

#[pyclass]
pub struct WorkflowBuilder {
    workflow_name: String,
    root_scope: Arc<TaskScope>,
    tasks: Mutex<Vec<TaskStep>>,
}

#[pymethods]
impl WorkflowBuilder {
    #[new]
    pub fn new(name: String) -> Self {
        let root_scope = TaskScope::new("root".to_string(), None);
        Self {
            workflow_name: name,
            root_scope,
            tasks: Mutex::new(vec![]),
        }
    }

    pub fn root_scope(&self) -> TaskScopeRef {
        TaskScopeRef(self.root_scope.clone())
    }

    pub fn add_task(&self, name: String, scope: TaskScopeRef) -> PyResult<TaskStep> {
        if !Arc::ptr_eq(&scope.0, &self.root_scope) {
            return Err(PyException::new_err("Tasks can only be added to root scope"));
        }
        let step = TaskStep { name: name.clone(), scope: scope.0.clone() };
        self.tasks.lock().unwrap().push(step.clone());
        Ok(step)
    }

    pub fn build(&self) -> PyResult<String> {
        let tasks = self.tasks.lock().unwrap();
        let summary: Vec<String> = tasks.iter().map(|t| format!("{}", t.__str__())).collect();
        Ok(format!("Workflow: {}\nTasks:\n{}", self.workflow_name, summary.join("\n")))
    }

    pub fn __str__(&self) -> String {
        format!("WorkflowBuilder({})", self.workflow_name)
    }
}

/// Python module definition
#[pymodule]
fn workflow_rs(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<TaskScope>()?;
    m.add_class::<TaskScopeRef>()?;
    m.add_class::<TaskStep>()?;
    m.add_class::<WorkflowBuilder>()?;
    Ok(())
}
