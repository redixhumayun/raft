use std::time::{Duration, Instant};

pub trait Clock {
    fn now(&self) -> Instant;
}

pub struct RealClock;
impl Clock for RealClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

pub struct MockClock {
    pub current_time: Instant,
}

impl MockClock {
    pub fn advance(&mut self, duration: Duration) {
        self.current_time += duration;
    }
}

impl Clock for MockClock {
    fn now(&self) -> Instant {
        self.current_time
    }
}
