use bitflags::bitflags;

// see https://redis.io/commands/client-list/ for client flags
bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct ClientFlags : u32 {
        const CloseASAP = 1_u32 << 0;
        const Blocked = 1_u32 << 1;
        const CloseAfterWrtingReply = 1_u32 << 2;
        const WatchModified = 1_u32 << 3;
        const Eviction = 1_u32 << 4;
        const WaitingVMIO = 1_u32 << 5;
        const Master = 1_u32 << 6;
        const None = 1_u32 << 7;
        const Monitor = 1_u32 << 8;
        const Subscriber = 1_u32 << 9;
        const ReadOnly = 1_u32 << 10;
        const Replicas = 1_u32 << 11;
        const Unblocked = 1_u32 << 12;
        const Unix = 1_u32 << 13;
        const Trasactions = 1_u32 << 14;
        const KeysTracing = 1_u32 << 15;
        const NotTouchLRULFU = 1_u32 << 16;
        const TracingInvalid = 1_u32 << 17;
        const Broadcase = 1_u32 << 18;
    }
}