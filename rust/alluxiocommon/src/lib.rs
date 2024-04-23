use pyo3::prelude::*;
use pyo3::types::PyList;
use std::{cmp, thread, time};
use std::error::Error;
use std::sync::Arc;
use bytes::Bytes;
use tokio::runtime::{Builder, Runtime};
use reqwest::blocking;
use reqwest::blocking::Client;
use rayon::{ThreadPool, ThreadPoolBuilder, ThreadPoolBuildError};
use env_logger::Env;

use pyo3::{
    exceptions::PyIOError,
    exceptions::PyIndexError,
    exceptions::PyValueError,
    exceptions::PyTypeError,
    exceptions::PyException,
    prelude::{pymodule, PyModule, PyResult, Python},
    types::PyBytes,
    buffer::PyBuffer,
    PyErr,
};

const DEFAULT_THREADPOOL_NAME: &str = "ALLUXIOCOMMON";


#[pyclass(name = "_DataManager", module = "_lib")]
pub struct DataManager {
    max_threads: usize,
    ondemand_pool: bool,
    thread_pool: Option<Arc<ThreadPool>>,
    request_client: Option<Arc<Client>>,
}

#[pymethods]
impl DataManager {
    #[new]
    fn new(
        max_concurrency: usize,
        ondemand_pool: Option<bool>
    ) -> PyResult<Self> {
        match ondemand_pool {
            Some(is_ondemand_pool) => {
                if (!is_ondemand_pool) {
                    // Create reqwest client (The Client holds a connection pool internally, create to reuse this Client obj).
                    let client = Client::new();
                    // Create threadpool
                    let pool_result =
                        create_pool(max_concurrency, String::from(DEFAULT_THREADPOOL_NAME));
                    match pool_result {
                        Ok(pool) => {
                            return Ok(Self {
                                max_threads: max_concurrency,
                                ondemand_pool: is_ondemand_pool,
                                thread_pool: Option::Some(Arc::new(pool)),
                                request_client: Option::Some(Arc::new(client)),
                            });
                        },
                        Err(err) => {
                            return Err(PyValueError::new_err(err.to_string()));
                        },
                    }
                } else {
                    return Ok(Self {
                        max_threads: max_concurrency,
                        ondemand_pool: true,
                        thread_pool: None,
                        request_client: None,
                    });
                }
            },
            None => {
                return Ok(Self {
                    max_threads: max_concurrency,
                    ondemand_pool: true,
                    thread_pool: None,
                    request_client: None,
                });
            }
        }
        println!("instantiate _DataManager");
    }

    fn make_multi_http_req(self_: PyRef<'_, Self>, urls: Vec<String>) -> PyResult<PyObject> {
        println!("on demand pool and client!");
        let num_reqs = urls.len();
        let mut content_results = Vec::with_capacity(num_reqs);
        let mut senders = Vec::with_capacity(num_reqs);
        for _ in 0..num_reqs {
            senders.push(None);
        }

        // let Some(kmeans) = self.trained_kmeans.as_ref() else {
        //     return Err(PyRuntimeError::new_err("KMeans must fit (train) first"));
        // };

        let thread_pool /*: Arc<ThreadPool>*/ = match self_.ondemand_pool {
            true => {
                match create_pool(cmp::min(self_.max_threads, num_reqs),
                                  String::from(DEFAULT_THREADPOOL_NAME))
                {
                    Ok(pool) => Arc::new(pool),
                    Err(err) => {
                        PyException::new_err(err.to_string())
                            .restore(self_.py());
                        return Err(PyErr::fetch(self_.py()));
                    },
                }
            },
            false => {
                Arc::clone(&(self_.thread_pool.as_ref().unwrap())) // it can't be None here once instantiated
            }
        };

        let request_client = match self_.ondemand_pool {
            true => {
                Arc::new(Client::new())
            },
            false => {
                Arc::clone(&(self_.request_client.as_ref().unwrap()))
            }
        };
        for i in 0..num_reqs {
            let url_owned = urls[i].to_owned();
            // let client_clone = Arc::clone(&(self_.request_client));
            let client_clone = Arc::clone(&(request_client));
            let (send, recv) = tokio::sync::oneshot::channel();
            let install_res = thread_pool.install(move || -> Result<(), reqwest::Error> {
                // println!("request idx:{}", i);
                let body = perform_http_get(url_owned.as_str(), client_clone.as_ref());
                send.send(body).unwrap();
                Ok(())
            });
            match install_res {
                Ok(_success) => {
                    //DO NOTHING
                },
                Err(err) => {
                    PyException::new_err(err.to_string())
                        .restore(self_.py());
                    return Err(PyErr::fetch(self_.py()));
                },
            }
            senders[i] = Some(recv);
        }
        for sender in senders {
            let result = sender.unwrap().blocking_recv();
            match result {
                Ok(content_result) => {
                    content_results.push(content_result);
                },
                Err(err) => {
                    PyException::new_err(err.to_string())
                        .restore(self_.py());
                    return Err(PyErr::fetch(self_.py()));
                }
            }
            // let result = sender.unwrap().blocking_recv().unwrap();
            // content_results.push(result);
        }
        let mut concatenated_data: Vec<u8> = Vec::new();
        for content_result in &content_results {
            match content_result {
                Ok(content) => {
                    concatenated_data.extend(content);
                },
                Err(err) => {
                    let err_str = err.to_string();
                    PyException::new_err(format!("Error in getting result, {}", err_str))
                        .restore(self_.py());
                    return Err(PyErr::fetch(self_.py()));
                }
            }
        }
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

fn create_pool(num_threads: usize, thread_name_prefix: String) -> Result<ThreadPool, Box<ThreadPoolBuildError>> {
    let name_prefix = thread_name_prefix.clone();
    match rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .thread_name(move |i| format!("{}-{}", &name_prefix, i))
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
    use log::info;

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[test]
    fn test_logger() {
        init();
        info!("This record will be captured by `cargo test`");
        assert_eq!(2, 1 + 1);
    }

    #[test]
    fn test_example() {
        init();
        let client = Client::new();
        let res = perform_http_get("http://127.0.0.1:1000", &client);
        match res {
            Ok(success) => {
                print!("SUCCESS!");
            },
            Err(e) => {
                print!("error!:{}", e);
            }
        }
        print!("test_example");
    }
}


#[pymodule]
fn alluxiocommon(_py: Python, m: &PyModule) -> PyResult<()> {
    let env = Env::new()
        .filter_or("ALLUXIOCOMMON_LOG", "warn");
    env_logger::init_from_env(env);

    m.add_class::<DataManager>()?;
    Ok(())
}
