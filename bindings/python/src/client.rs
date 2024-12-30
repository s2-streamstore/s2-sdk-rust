use std::sync::Arc;

use crate::types::{
    AppendRecordMapper, PyAppendRecord, PyBasinConfig, PyBasinInfo, PyClientConfig,
    PyListBasinsResponse, PyListStreamsResponse, PyReadOutput, PyStreamConfig, PyStreamInfo,
    PyStreamingAppendOutput, PyStreamingReadOutput,
};
use futures::{Stream, StreamExt};
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3_async_runtimes::tokio as pyo3_tokio;
use s2::batching::{AppendRecordsBatchingOpts, AppendRecordsBatchingStream};
use s2::types::AppendRecord;
use s2::types::{
    BasinName, CreateBasinRequest, CreateStreamRequest, DeleteBasinRequest, DeleteStreamRequest,
    ListBasinsRequest, ListStreamsRequest, ReadRequest, ReadSessionRequest,
    ReconfigureBasinRequest, ReconfigureStreamRequest,
};
use s2::{BasinClient, Client, StreamClient};

// MYDO: revisit PyException

#[pyclass(name = "Client")]
#[derive(Clone)]
pub struct PyClient(Client);

#[pymethods]
impl PyClient {
    #[new]
    fn new(config: &Bound<'_, PyClientConfig>) -> PyResult<Self> {
        let config: PyClientConfig = config.extract()?;
        let client = pyo3_tokio::get_runtime().block_on(async { Client::new(config.0) });
        Ok(Self(client))
    }

    fn basin_client<'p>(&'p self, _py: Python<'p>, basin_name: String) -> PyResult<PyBasinClient> {
        let basin_name = BasinName::try_from(basin_name).unwrap(); // MYDO: fix all these unwraps
        let basin_client = self.0.basin_client(basin_name);
        Ok(PyBasinClient(basin_client))
    }

    fn list_basins<'p>(
        &'p self,
        py: Python<'p>,
        prefix: String,
        start_after: String,
        limit: usize,
    ) -> PyResult<Bound<'p, PyAny>> {
        let request = ListBasinsRequest {
            prefix,
            start_after,
            limit,
        };
        let client = self.0.clone();
        pyo3_tokio::future_into_py(py, async move {
            let response: PyListBasinsResponse = client
                .list_basins(request)
                .await
                .map_err(|err| PyException::new_err(format!("{:?}", err)))?
                .try_into()?;
            Ok(response)
        })
    }

    fn create_basin<'p>(
        &'p self,
        py: Python<'p>,
        basin_name: String,
    ) -> PyResult<Bound<'p, PyAny>> {
        let basin_name = BasinName::try_from(basin_name).unwrap();
        let request = CreateBasinRequest::new(basin_name);
        let client = self.0.clone();
        pyo3_tokio::future_into_py(py, async move {
            let response: PyBasinInfo = client
                .create_basin(request)
                .await
                .map_err(|err| PyException::new_err(format!("{:?}", err)))?
                .try_into()?;
            Ok(response)
        })
    }

    fn delete_basin<'p>(
        &'p self,
        py: Python<'p>,
        basin_name: String,
    ) -> PyResult<Bound<'p, PyAny>> {
        let basin_name = BasinName::try_from(basin_name).unwrap();
        let request = DeleteBasinRequest::new(basin_name);
        let client = self.0.clone();
        pyo3_tokio::future_into_py(py, async move {
            client
                .delete_basin(request)
                .await
                .map_err(|err| PyException::new_err(format!("{:?}", err)))
        })
    }

    fn get_basin_config<'p>(
        &'p self,
        py: Python<'p>,
        basin_name: String,
    ) -> PyResult<Bound<'p, PyAny>> {
        let basin_name = BasinName::try_from(basin_name).unwrap();
        let client = self.0.clone();
        pyo3_tokio::future_into_py(py, async move {
            let response: PyBasinConfig = client
                .get_basin_config(basin_name)
                .await
                .map_err(|err| PyException::new_err(format!("{:?}", err)))?
                .into();
            Ok(response)
        })
    }

    #[pyo3(signature = (basin_name, **kwargs), text_signature="(basin_name)")]
    fn reconfigure_basin<'p>(
        &'p self,
        py: Python<'p>,
        basin_name: String,
        kwargs: Option<&Bound<'_, PyDict>>, // MYDO: can get rid of this type and go flat with Option<T>?
    ) -> PyResult<Bound<'p, PyAny>> {
        let basin_name = BasinName::try_from(basin_name).unwrap();
        let request = ReconfigureBasinRequest::new(basin_name);
        if let Some(_kwargs) = kwargs {
            // MYDO
        }
        let client = self.0.clone();
        pyo3_tokio::future_into_py(py, async move {
            let response: PyBasinConfig = client
                .reconfigure_basin(request)
                .await
                .map_err(|err| PyException::new_err(format!("{:?}", err)))?
                .into();
            Ok(response)
        })
    }
}

#[pyclass(name = "BasinClient")]
pub struct PyBasinClient(BasinClient);

#[pymethods]
impl PyBasinClient {
    fn stream_client<'p>(&'p self, _py: Python<'p>, stream_name: String) -> PyStreamClient {
        PyStreamClient(self.0.stream_client(stream_name))
    }

    fn list_streams<'p>(
        &'p self,
        py: Python<'p>,
        prefix: String,
        start_after: String,
        limit: usize,
    ) -> PyResult<Bound<'p, PyAny>> {
        let request = ListStreamsRequest {
            prefix,
            start_after,
            limit,
        };
        let client = self.0.clone();
        pyo3_tokio::future_into_py(py, async move {
            let response: PyListStreamsResponse = client
                .list_streams(request)
                .await
                .map_err(|err| PyException::new_err(format!("{:?}", err)))?
                .try_into()?;
            Ok(response)
        })
    }

    #[pyo3(signature = (stream_name, **kwargs), text_signature="(stream_name)")]
    fn create_stream<'p>(
        &'p self,
        py: Python<'p>,
        stream_name: String,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let request = CreateStreamRequest::new(stream_name);
        if let Some(_kwargs) = kwargs {
            // MYDO
        }
        let client = self.0.clone();
        pyo3_tokio::future_into_py(py, async move {
            let response: PyStreamInfo = client
                .create_stream(request)
                .await
                .map_err(|err| PyException::new_err(format!("{:?}", err)))?
                .into();
            Ok(response)
        })
    }

    fn delete_stream<'p>(
        &'p self,
        py: Python<'p>,
        stream_name: String,
    ) -> PyResult<Bound<'p, PyAny>> {
        let request = DeleteStreamRequest::new(stream_name);
        let client = self.0.clone();
        pyo3_tokio::future_into_py(py, async move {
            client
                .delete_stream(request)
                .await
                .map_err(|err| PyException::new_err(format!("{:?}", err)))
        })
    }

    fn get_stream_config<'p>(
        &'p self,
        py: Python<'p>,
        stream_name: String,
    ) -> PyResult<Bound<'p, PyAny>> {
        let client = self.0.clone();
        pyo3_tokio::future_into_py(py, async move {
            let response: PyStreamConfig = client
                .get_stream_config(stream_name)
                .await
                .map_err(|err| PyException::new_err(format!("{:?}", err)))?
                .into();
            Ok(response)
        })
    }

    #[pyo3(signature = (stream_name, **kwargs), text_signature="(stream_name)")]
    fn reconfigure_stream<'p>(
        &'p self,
        py: Python<'p>,
        stream_name: String,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let request = ReconfigureStreamRequest::new(stream_name);
        if let Some(_kwargs) = kwargs {
            // MYDO
        }
        let client = self.0.clone();
        pyo3_tokio::future_into_py(py, async move {
            let response: PyStreamConfig = client
                .reconfigure_stream(request)
                .await
                .map_err(|err| PyException::new_err(format!("{:?}", err)))?
                .into();
            Ok(response)
        })
    }
}

#[pyclass(name = "StreamClient")]
pub struct PyStreamClient(StreamClient);

#[pymethods]
impl PyStreamClient {
    fn check_tail<'p>(&'p self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let client = self.0.clone();
        pyo3_tokio::future_into_py(py, async move {
            client
                .check_tail()
                .await
                .map_err(|err| PyException::new_err(format!("{:?}", err)))
        })
    }

    #[pyo3(signature = (start_seq_num, **kwargs), text_signature="(start_seq_num)")]
    fn read<'p>(
        &'p self,
        py: Python<'p>,
        start_seq_num: u64,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Bound<'p, PyAny>> {
        //let start_seq_num: u64 = start_seq_num.extract()?;
        let request = ReadRequest::new(start_seq_num);
        let client = self.0.clone();
        if let Some(_kwargs) = kwargs {
            // MYDO
        }
        pyo3_tokio::future_into_py(py, async move {
            let response: PyReadOutput = client
                .read(request)
                .await
                .map_err(|err| PyException::new_err(format!("{:?}", err)))?
                .into();
            Ok(response)
        })
    }

    #[pyo3(signature = (start_seq_num, **kwargs), text_signature="(start_seq_num)")]
    fn read_session<'p>(
        &'p self,
        py: Python<'p>,
        start_seq_num: u64,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Bound<'p, PyAny>> {
        //let start_seq_num: u64 = start_seq_num.extract()?;
        let request = ReadSessionRequest::new(start_seq_num);
        let client = self.0.clone();
        if let Some(_kwargs) = kwargs {
            // MYDO
        }
        pyo3_tokio::future_into_py(py, async move {
            let response: PyStreamingReadOutput = client
                .read_session(request)
                .await
                .map_err(|err| PyException::new_err(format!("{:?}", err)))?
                .into();
            Ok(response)
        })
    }

    // MYDO: &Bound or Bound in general?

    // MYDO: WIP
    // fn append_session<'p>(
    //     &'p self,
    //     py: Python<'p>,
    //     append_inputs: Bound<'p, PyAny>,
    // ) -> PyResult<Bound<'p, PyAny>> {
    //     let request = Python::with_gil(|py| {
    //         let stream = Box::new(pyo3_tokio::into_stream_v2(append_inputs).unwrap().map(|r| {
    //             let r: AppendRecord = r.extract::<PyAppendRecord>(py).unwrap().try_into().unwrap();
    //             r
    //         }));
    //         AppendRecordsBatchingStream::new(stream, AppendRecordsBatchingOpts::default())
    //     });
    //     let client = self.0.clone();
    //     pyo3_tokio::future_into_py(py, async move {
    //         let response: PyStreamingAppendOutput = client
    //             .append_session(request)
    //             .await
    //             .map_err(|err| PyException::new_err(format!("{:?}", err)))?
    //             .into();
    //         Ok(response)
    //     })
    // }
}
