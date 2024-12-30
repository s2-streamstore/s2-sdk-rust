use std::pin::Pin;

use bytes::Bytes;
use futures::Stream;
use futures::TryStreamExt;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::types::PyIterator;
use pyo3::types::PyList;
use pyo3::types::PyString;
use pyo3_async_runtimes::tokio as pyo3_tokio;
use s2::client::ClientError;
use s2::types::{
    AppendOutput, AppendRecord, BasinConfig, BasinInfo, BasinState, Header, ListBasinsResponse,
    ListStreamsResponse, ReadOutput, SequencedRecord, SequencedRecordBatch, StorageClass,
    StreamConfig, StreamInfo,
};
use s2::ClientConfig;
use std::sync::Arc;
use tokio::sync::Mutex;

#[pyclass(name = "ClientConfig")]
#[derive(Clone)]
pub struct PyClientConfig(pub ClientConfig);

// MYDO: revisit cfg plumbing
#[pymethods]
impl PyClientConfig {
    #[new]
    #[pyo3(signature = (auth_token, **kwargs), text_signature="(auth_token)")]
    fn new(auth_token: &Bound<'_, PyString>, kwargs: Option<&Bound<'_, PyDict>>) -> PyResult<Self> {
        let token: String = auth_token.extract()?;
        let config = ClientConfig::new(token);
        if let Some(_kwargs) = kwargs {
            // TODO
        }
        Ok(Self(config))
    }
}

#[pyclass(eq, eq_int, name = "BasinState", frozen)]
#[derive(Clone, PartialEq)]
pub enum PyBasinState {
    Unspecified,
    Active,
    Creating,
    Deleting,
}

impl From<BasinState> for PyBasinState {
    fn from(state: BasinState) -> Self {
        match state {
            BasinState::Unspecified => Self::Unspecified,
            BasinState::Active => Self::Active,
            BasinState::Creating => Self::Creating,
            BasinState::Deleting => Self::Deleting,
        }
    }
}

// MYDO: make other structs like this wherever applicable
#[pyclass(name = "BasinInfo", frozen)]
pub struct PyBasinInfo {
    #[pyo3(get)]
    name: Py<PyString>,
    #[pyo3(get)]
    scope: Py<PyString>,
    #[pyo3(get)]
    cell: Py<PyString>,
    #[pyo3(get)]
    state: Py<PyBasinState>,
}

#[pymethods]
impl PyBasinInfo {
    fn __repr__(&self) -> String {
        format!(
            "BasinInfo(name={}, scope={}, cell={})",
            self.name, self.scope, self.cell
        )
    }
}

impl TryFrom<BasinInfo> for PyBasinInfo {
    type Error = PyErr;

    fn try_from(info: BasinInfo) -> PyResult<Self> {
        let state: PyBasinState = info.state.into();
        Python::with_gil(|py| {
            Ok(Self {
                name: info.name.into_pyobject(py)?.into(),
                scope: info.scope.into_pyobject(py)?.into(),
                cell: info.cell.into_pyobject(py)?.into(),
                state: Py::new(py, state)?,
            })
        })
    }
}

#[pyclass(name = "StreamInfo", frozen)]
pub struct PyStreamInfo {
    #[pyo3(get)]
    name: String,
    #[pyo3(get)]
    created_at: u32,
    #[pyo3(get)]
    deleted_at: Option<u32>,
}

impl From<StreamInfo> for PyStreamInfo {
    fn from(info: StreamInfo) -> Self {
        Self {
            name: info.name,
            created_at: info.created_at,
            deleted_at: info.deleted_at,
        }
    }
}

#[pyclass(name = "ListBasinsResponse", frozen)]
pub struct PyListBasinsResponse {
    #[pyo3(get)]
    basins: Py<PyList>,
    #[pyo3(get)]
    has_more: bool,
}

impl TryFrom<ListBasinsResponse> for PyListBasinsResponse {
    type Error = PyErr;

    fn try_from(response: ListBasinsResponse) -> PyResult<Self> {
        let basins: PyResult<Vec<PyBasinInfo>> =
            response.basins.into_iter().map(|b| b.try_into()).collect();
        Python::with_gil(|py| {
            Ok(Self {
                basins: PyList::new(py, basins?)?.into(),
                has_more: response.has_more,
            })
        })
    }
}

#[pyclass(name = "ListStreamsResponse")]
pub struct PyListStreamsResponse {
    #[pyo3(get)]
    streams: Py<PyList>,
    #[pyo3(get)]
    has_more: bool,
}

impl TryFrom<ListStreamsResponse> for PyListStreamsResponse {
    type Error = PyErr;

    fn try_from(response: ListStreamsResponse) -> PyResult<Self> {
        let streams: Vec<PyStreamInfo> = response.streams.into_iter().map(|s| s.into()).collect();
        Python::with_gil(|py| {
            Ok(Self {
                streams: PyList::new(py, streams)?.into(),
                has_more: response.has_more,
            })
        })
    }
}

#[pyclass(name = "BasinConfig")]
pub struct PyBasinConfig {
    #[pyo3(get)]
    default_stream_config: Option<PyStreamConfig>,
}

impl From<BasinConfig> for PyBasinConfig {
    fn from(config: BasinConfig) -> Self {
        Self {
            default_stream_config: config.default_stream_config.map(|c| c.into()),
        }
    }
}

#[pyclass(name = "StreamConfig")]
#[derive(Clone)]
pub struct PyStreamConfig {
    #[pyo3(get)]
    storage_class: PyStorageClass,
}

impl From<StreamConfig> for PyStreamConfig {
    fn from(config: StreamConfig) -> Self {
        Self {
            storage_class: config.storage_class.into(),
        }
    }
}

#[pyclass(eq, eq_int, name = "StorageClass")]
#[derive(Clone, PartialEq)]
pub enum PyStorageClass {
    Unspecified,
    Standard,
    Express,
}

impl From<StorageClass> for PyStorageClass {
    fn from(class: StorageClass) -> Self {
        match class {
            StorageClass::Unspecified => Self::Unspecified,
            StorageClass::Standard => Self::Standard,
            StorageClass::Express => Self::Express,
        }
    }
}

#[pyclass]
pub struct PyStreamingReadOutput(
    Arc<Mutex<Pin<Box<dyn Stream<Item = Result<ReadOutput, ClientError>> + Send>>>>,
);

#[pymethods]
impl PyStreamingReadOutput {
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<Self> {
        slf
    }

    fn __anext__(slf: PyRefMut<'_, Self>) -> PyResult<Bound<PyAny>> {
        let stream = slf.0.clone();
        pyo3_tokio::future_into_py(slf.py(), async move {
            let mut stream = stream.lock().await;
            let next: Option<PyReadOutput> = stream
                .try_next()
                .await
                .map_err(|err| PyException::new_err(format!("{:?}", err)))?
                .map(|o| o.into());
            Ok(next)
        })
    }
}

impl From<Pin<Box<dyn Stream<Item = Result<ReadOutput, ClientError>> + Send>>>
    for PyStreamingReadOutput
{
    fn from(stream: Pin<Box<dyn Stream<Item = Result<ReadOutput, ClientError>> + Send>>) -> Self {
        Self(Arc::new(Mutex::new(stream)))
    }
}

#[pyclass(name = "ReadOutput")]
pub enum PyReadOutput {
    Batch(PySequencedRecordBatch),
    FirstSeqNum(u64),
    NextSeqNum(u64),
}

impl From<ReadOutput> for PyReadOutput {
    fn from(output: ReadOutput) -> Self {
        match output {
            ReadOutput::Batch(batch) => Self::Batch(batch.into()),
            ReadOutput::FirstSeqNum(seq_num) => Self::FirstSeqNum(seq_num),
            ReadOutput::NextSeqNum(seq_num) => Self::NextSeqNum(seq_num),
        }
    }
}

#[pyclass(name = "SequencedRecordBatch")]
#[derive(Clone)]
pub struct PySequencedRecordBatch {
    #[pyo3(get)]
    records: Vec<PySequencedRecord>,
}

impl From<SequencedRecordBatch> for PySequencedRecordBatch {
    fn from(batch: SequencedRecordBatch) -> Self {
        Self {
            records: batch.records.into_iter().map(|r| r.into()).collect(),
        }
    }
}

#[pyclass(name = "SequencedRecord")]
#[derive(Clone)]
struct PySequencedRecord {
    #[pyo3(get)]
    seq_num: u64,
    #[pyo3(get)]
    headers: Vec<PyHeader>,
    #[pyo3(get)]
    body: Vec<u8>,
}

impl From<SequencedRecord> for PySequencedRecord {
    fn from(record: SequencedRecord) -> Self {
        Self {
            seq_num: record.seq_num,
            headers: record.headers.into_iter().map(|h| h.into()).collect(),
            body: record.body.into(),
        }
    }
}

#[pyclass(name = "Header")]
#[derive(Clone)]
struct PyHeader {
    #[pyo3(get, set)]
    name: Vec<u8>,
    #[pyo3(get, set)]
    value: Vec<u8>,
}

impl From<Header> for PyHeader {
    fn from(header: Header) -> Self {
        Self {
            name: header.name.into(),
            value: header.value.into(),
        }
    }
}

impl From<PyHeader> for Header {
    fn from(header: PyHeader) -> Self {
        Self {
            name: header.name.into(),
            value: header.value.into(),
        }
    }
}

#[pyclass(name = "AppendInput")]
struct PyAppendInput {}

struct PyAppendRecordBatch {}

#[pyclass(name = "AppendRecord")]
pub struct PyAppendRecord {
    #[pyo3(get, set)]
    headers: Vec<PyHeader>,
    #[pyo3(get, set)]
    body: Vec<u8>,
}

impl FromPyObject<'_> for PyAppendRecord {
    fn extract_bound(ob: &Bound<'_, PyAny>) -> PyResult<Self> {
        let record = ob.downcast::<PyAppendRecord>()?;
        let headers: Vec<PyHeader> = record.getattr("headers")?.extract()?;
        let body: Vec<u8> = record.getattr("body")?.extract()?;
        Ok(PyAppendRecord { headers, body })
    }
}

impl TryFrom<PyAppendRecord> for AppendRecord {
    type Error = PyErr;

    fn try_from(record: PyAppendRecord) -> PyResult<Self> {
        Ok(Self::new(record.body)
            .map_err(|err| PyException::new_err(format!("{:?}", err)))?
            .with_headers(
                record
                    .headers
                    .into_iter()
                    .map(|h| h.into())
                    .collect::<Vec<_>>(),
            )
            .map_err(|err| PyException::new_err(format!("{:?}", err)))?)
    }
}

// MYDO: add intern! to all getattrs

pub struct AppendRecordMapper {
    pub headers: Vec<Header>,
    pub body: Vec<Bytes>,
}

impl FromPyObject<'_> for AppendRecordMapper {
    fn extract_bound(ob: &Bound<'_, PyAny>) -> PyResult<Self> {
        let record = ob.downcast::<PyAppendRecord>()?;
        let headers: Vec<PyHeader> = record.getattr("headers")?.extract()?;
        let body: Vec<Vec<u8>> = record.getattr("body")?.extract()?;
        Ok(AppendRecordMapper {
            headers: headers.into_iter().map(|h| h.into()).collect(),
            body: body.into_iter().map(|b| b.into()).collect(),
        })
    }
}

#[pyclass(name = "AppendOutput")]
pub struct PyAppendOutput {
    start_seq_num: u64,
    end_seq_num: u64,
    next_seq_num: u64,
}

impl From<AppendOutput> for PyAppendOutput {
    fn from(output: AppendOutput) -> Self {
        Self {
            start_seq_num: output.start_seq_num,
            end_seq_num: output.end_seq_num,
            next_seq_num: output.next_seq_num,
        }
    }
}

// MYDO: https://pyo3.rs/v0.23.3/class.html#no-generic-parameters -- maybe PyStreamingOutput<T> for Append and Read
#[pyclass]
pub struct PyStreamingAppendOutput(
    Arc<Mutex<Pin<Box<dyn Stream<Item = Result<AppendOutput, ClientError>> + Send>>>>,
);

#[pymethods]
impl PyStreamingAppendOutput {
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<Self> {
        slf
    }

    fn __anext__(slf: PyRefMut<'_, Self>) -> PyResult<Bound<PyAny>> {
        let stream = slf.0.clone();
        pyo3_tokio::future_into_py(slf.py(), async move {
            // MYDO: revisit StopAsyncIteration i.e. should raise it explicity to stop the iteration?
            let mut stream = stream.lock().await;
            let next: Option<PyAppendOutput> = stream
                .try_next()
                .await
                .map_err(|err| PyException::new_err(format!("{:?}", err)))?
                .map(|o| o.into());
            Ok(next)
        })
    }
}

impl From<Pin<Box<dyn Stream<Item = Result<AppendOutput, ClientError>> + Send>>>
    for PyStreamingAppendOutput
{
    fn from(stream: Pin<Box<dyn Stream<Item = Result<AppendOutput, ClientError>> + Send>>) -> Self {
        Self(Arc::new(Mutex::new(stream)))
    }
}
