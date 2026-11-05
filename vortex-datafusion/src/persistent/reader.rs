// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Factory for creating [`VortexReadAt`] instances for [`PartitionedFile`]s.

use std::fmt::Debug;
use std::sync::Arc;

use datafusion_common::Result as DFResult;
use datafusion_datasource::PartitionedFile;
use object_store::ObjectStore;
use object_store::path::Path;
use vortex::array::memory::MemorySessionExt;
use vortex::io::CoalesceConfig;
use vortex::io::VortexReadAt;
use vortex::io::object_store::ObjectStoreReadAt;
use vortex::io::session::RuntimeSessionExt;
use vortex::session::VortexSession;

/// Factory to create [`VortexReadAt`] instances for a `PartitionedFile`.
///
/// Plug a custom implementation into [`VortexSource::with_vortex_reader_factory`]
/// when the default object-store reader is not sufficient, for example to:
///
/// - reuse an application-level metadata cache,
/// - wrap reads with custom authentication or routing,
/// - coalesce I/O in a remote storage layer.
///
/// [`VortexSource::with_vortex_reader_factory`]: crate::VortexSource::with_vortex_reader_factory
pub trait VortexReaderFactory: Debug + Send + Sync + 'static {
    /// Create a reader for a target object.
    fn create_reader(
        &self,
        file: &PartitionedFile,
        session: &VortexSession,
    ) -> DFResult<Arc<dyn VortexReadAt>>;
}

/// Default [`VortexReaderFactory`] backed by DataFusion's [`ObjectStore`].
///
/// This is the reader used by [`crate::VortexSource`] and
/// [`crate::VortexFormat`] unless a
/// custom factory is supplied. It works with any object store that DataFusion
/// has registered for the scan.
#[derive(Debug)]
pub struct DefaultVortexReaderFactory {
    object_store: Arc<dyn ObjectStore>,
    coalesce_distance_bytes: Option<u64>,
}

impl DefaultVortexReaderFactory {
    /// Creates a new factory from an [`ObjectStore`].
    ///
    /// # Example
    ///
    /// ```rust
    /// # use std::sync::Arc;
    /// # use object_store::memory::InMemory;
    /// use vortex_datafusion::reader::DefaultVortexReaderFactory;
    ///
    /// let factory = DefaultVortexReaderFactory::new(Arc::new(InMemory::new()));
    /// # let _ = factory;
    /// ```
    pub fn new(object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            object_store,
            coalesce_distance_bytes: None,
        }
    }

    /// Overrides the read-coalescing distance (bytes). `None` keeps the object-store default.
    pub fn with_coalesce_distance(mut self, distance_bytes: Option<u64>) -> Self {
        self.coalesce_distance_bytes = distance_bytes;
        self
    }
}

/// Builds the object-store reader used by schema inference, stats, and the default scan factory.
pub fn object_store_reader(
    store: Arc<dyn ObjectStore>,
    path: Path,
    session: &VortexSession,
    coalesce_distance_bytes: Option<u64>,
) -> ObjectStoreReadAt {
    let reader =
        ObjectStoreReadAt::new_with_allocator(store, path, session.handle(), session.allocator());
    match coalesce_distance_bytes.filter(|d| *d > 0) {
        Some(distance) => {
            let base = CoalesceConfig::object_storage();
            reader.with_coalesce_config(CoalesceConfig::new(distance, base.max_size.max(distance)))
        }
        None => reader,
    }
}

impl VortexReaderFactory for DefaultVortexReaderFactory {
    fn create_reader(
        &self,
        file: &PartitionedFile,
        session: &VortexSession,
    ) -> DFResult<Arc<dyn VortexReadAt>> {
        Ok(Arc::new(object_store_reader(
            Arc::clone(&self.object_store),
            file.path().clone(),
            session,
            self.coalesce_distance_bytes,
        )) as _)
    }
}

#[cfg(test)]
mod tests {
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use vortex::VortexSessionDefault;
    use vortex::io::CoalesceConfig;
    use vortex::io::VortexReadAt;
    use vortex::io::session::RuntimeSessionExt;

    use super::object_store_reader;

    #[test]
    fn coalesce_distance_overrides_the_object_store_default() {
        let session = vortex::session::VortexSession::default().with_tokio();
        let store = std::sync::Arc::new(InMemory::new());
        let plain = object_store_reader(store.clone(), Path::from("a.vortex"), &session, None);
        assert_eq!(
            plain.coalesce_config().map(|c| c.distance),
            Some(CoalesceConfig::object_storage().distance)
        );
        let wide = object_store_reader(store, Path::from("b.vortex"), &session, Some(256 * 1024));
        assert_eq!(wide.coalesce_config().map(|c| c.distance), Some(256 * 1024));
    }
}
