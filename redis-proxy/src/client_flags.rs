use bitflags::bitflags;

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct SessionFlags : u32 {
        const None = 0_u32;
        const InTrasactions = 1_u32 << 0;
        const InWatching = 1_u32 << 1;
    }
}