mod client;
mod types;

use pyo3::prelude::*;

#[pymodule]
fn streamstore(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<types::PyClientConfig>()?;
    m.add_class::<client::PyClient>()?;
    m.add_class::<client::PyBasinClient>()?;
    Ok(())
}
