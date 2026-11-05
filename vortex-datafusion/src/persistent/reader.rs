// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Factory for creating [`VortexReadAt`] instances for [`PartitionedFile`]s.

use std::fmt::Debug;
use std::sync::Arc;

use datafusion_common::Result as DFResult;
use datafusion_datasource::PartitionedFile;
use object_store::ObjectStore;
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
    coalesce_config: Option<CoalesceConfig>,
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
            coalesce_config: None,
        }
    }

    /// Sets the I/O coalescing of the readers this factory creates; `None` keeps the
    /// object-store reader's default (see [`CoalesceConfig::object_storage`]).
    pub fn with_coalesce_config(mut self, coalesce_config: Option<CoalesceConfig>) -> Self {
        self.coalesce_config = coalesce_config;
        self
    }
}

impl VortexReaderFactory for DefaultVortexReaderFactory {
    fn create_reader(
        &self,
        file: &PartitionedFile,
        session: &VortexSession,
    ) -> DFResult<Arc<dyn VortexReadAt>> {
        let reader = ObjectStoreReadAt::new_with_allocator(
            Arc::clone(&self.object_store),
            file.path().clone(),
            session.handle(),
            session.allocator(),
        );
        let reader = match self.coalesce_config {
            Some(config) => reader.with_coalesce_config(config),
            None => reader,
        };
        Ok(Arc::new(reader) as _)
    }
}

#[cfg(test)]
mod tests {
    use object_store::memory::InMemory;
    use vortex::VortexSessionDefault;
    use vortex::io::session::RuntimeSessionExt;

    use super::*;
    use crate::VortexTableOptions;

    fn reader(factory: &DefaultVortexReaderFactory) -> DFResult<Arc<dyn VortexReadAt>> {
        let session = VortexSession::default().with_tokio();
        let file = PartitionedFile::new("part.vortex", 0);
        factory.create_reader(&file, &session)
    }

    #[tokio::test]
    async fn factory_keeps_the_reader_default_without_a_config() -> DFResult<()> {
        let factory = DefaultVortexReaderFactory::new(Arc::new(InMemory::new()));
        let got = reader(&factory)?.coalesce_config();
        let want = CoalesceConfig::object_storage();
        assert_eq!(
            got.map(|c| (c.distance, c.max_size)),
            Some((want.distance, want.max_size))
        );
        Ok(())
    }

    #[tokio::test]
    async fn factory_passes_its_config_to_the_readers_it_creates() -> DFResult<()> {
        let factory = DefaultVortexReaderFactory::new(Arc::new(InMemory::new()))
            .with_coalesce_config(Some(CoalesceConfig::new(64 * 1024, 4 << 20)));
        let got = reader(&factory)?.coalesce_config();
        assert_eq!(
            got.map(|c| (c.distance, c.max_size)),
            Some((64 * 1024, 4 << 20))
        );
        Ok(())
    }

    #[test]
    fn table_options_build_the_config_only_when_set() {
        let mut opts = VortexTableOptions::default();
        assert!(opts.coalesce_config().is_none());

        opts.io_coalesce_distance_bytes = Some(64 * 1024);
        let got = opts.coalesce_config().expect("distance set");
        assert_eq!(got.distance, 64 * 1024);
        assert_eq!(got.max_size, CoalesceConfig::object_storage().max_size);

        opts.io_coalesce_max_size_bytes = Some(2 << 20);
        let got = opts.coalesce_config().expect("both set");
        assert_eq!((got.distance, got.max_size), (64 * 1024, 2 << 20));

        opts.io_coalesce_distance_bytes = None;
        let got = opts.coalesce_config().expect("max size set");
        assert_eq!(got.distance, CoalesceConfig::object_storage().distance);
        assert_eq!(got.max_size, 2 << 20);
    }
}
