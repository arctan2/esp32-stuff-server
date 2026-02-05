use super::allocators::{SimAllocator, INTERNAL_HEAP, PSRAM_HEAP};
pub const InternalMemory: SimAllocator<17> = SimAllocator(&INTERNAL_HEAP);
pub const ExternalMemory: SimAllocator<23> = SimAllocator(&PSRAM_HEAP);
