use pyo3::prelude::*;
use pyo3::types::PyList;
use std::{thread,time};
use std::error::Error;
use std::sync::Arc;
use bytes::Bytes;
use tokio::runtime::{Builder, Runtime};
use reqwest::blocking;
use reqwest::blocking::Client;
use rayon::{ThreadPool, ThreadPoolBuilder, ThreadPoolBuildError};

use pyo3::{
    exceptions::PyIOError,
    exceptions::PyIndexError,
    exceptions::PyValueError,
    exceptions::PyTypeError,
    prelude::{pymodule, PyModule, PyResult, Python},
    types::PyBytes,
    buffer::PyBuffer,
    PyErr,
};


#[pyclass(name = "_DataManager", module = "_lib")]
pub struct DataManager {
    // #[pyo3(get)]
    thread_num: usize,
    thread_pool: Arc<ThreadPool>,
    request_client: Arc<Client>,
}

#[pymethods]
impl DataManager {
    #[new]
    fn new(
        num_threads: usize
    ) -> PyResult<Self> {
        // Create reqwest client (The Client holds a connection pool internally, create to reuse this Client obj).
        let client = Client::new();
        // Create threadpool
        let pool_result = create_pool(num_threads);
        match pool_result {
            Ok(pool) => {
                Ok(Self {
                    thread_num: num_threads,
                    thread_pool: Arc::new(pool),
                    request_client: Arc::new(client),
                })
            },
            Err(err) => Err(PyValueError::new_err(err.to_string())),
        }
    }

    fn make_multi_http_req(self_: PyRef<'_, Self>, urls: Vec<String>) -> PyResult<PyObject> {
        let num_reqs = urls.len();
        let mut content_results = Vec::with_capacity(num_reqs);
        let mut senders = Vec::with_capacity(num_reqs);
        for _ in 0..num_reqs {
            senders.push(None);
        }

        for i in 0..num_reqs {
            let url_owned = urls[i].to_owned();
            let client_clone = Arc::clone(&(self_.request_client));
            let (send, recv) = tokio::sync::oneshot::channel();
            let install_res = self_.thread_pool.install(move || -> Result<(), reqwest::Error> {
                println!("request idx:{}", i);
                let body = perform_http_get(url_owned.as_str(), client_clone.as_ref());
                send.send(body).unwrap();
                Ok(())
            });
            match install_res {
                Ok(success) => {
                    //DO NOTHING
                },
                Err(err) => (), //[TODO] handle error !! PyValueError::new_err(err.to_string()),
            }
            senders[i] = Some(recv);
        }
        for sender in senders {
            let result = sender.unwrap().blocking_recv().unwrap();
            content_results.push(result);
        }
        println!("content_results len:{}", content_results.len());
        let mut concatenated_data: Vec<u8> = Vec::new();
        for content in &content_results {
            concatenated_data.extend(content.as_ref().unwrap());
        }
        // ?? what return type to python?
        let py = self_.py();
        Ok(PyBytes::new(self_.py(), &concatenated_data).to_object(self_.py()))
    }
}

fn type_name_of_val<T>(_: T) -> &'static str {
    std::any::type_name::<T>()
}

fn perform_http_get(url: &str, client: &Client) -> Result<Vec<u8>, reqwest::Error> {
    let bytes = client.get(url).send()?
        .bytes()?;
    let bytes_vec = bytes.to_vec(); // TODO! avoid copy here at least, one more additional copy in returning to py world
    Ok(bytes_vec)
}

fn create_pool(num_threads: usize) -> Result<ThreadPool, Box<ThreadPoolBuildError>> {
    match rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .build()
    {
        Err(e) => Err(Box::new(e.into())),
        Ok(pool) => Ok(pool),
    }
}

#[cfg(test)] // Indicates that the following functions are only compiled when running tests
mod tests {
    use super::*;
    use super::PyBuffer;
    use crate::Python;

    #[test]
    fn test_example() {
        print!("test_example");
    }
}


#[pymodule]
fn alluxiocommon(_py: Python, m: &PyModule) -> PyResult<()> {
    // let env = Env::new()
    //     .filter_or("ALLUXIOCOMMON_LOG", "warn")
    //     .write_style("ALLUXIOCOMMON_LOG_STYLE");
    // env_logger::init_from_env(env);

    m.add_class::<DataManager>()?;
    Ok(())
}
