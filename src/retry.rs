use std::time::Duration;

use rand::{Rng, rng};

#[derive(Debug, Clone, Copy)]
pub struct RetryBackoffBuilder {
    pub min_base_delay: Duration,
    pub max_base_delay: Duration,
    pub max_retries: u32,
}

impl Default for RetryBackoffBuilder {
    fn default() -> Self {
        Self {
            min_base_delay: Duration::from_millis(100),
            max_base_delay: Duration::from_secs(1),
            max_retries: 3,
        }
    }
}

impl RetryBackoffBuilder {
    pub fn with_min_base_delay(self, min_base_delay: Duration) -> Self {
        Self {
            min_base_delay,
            ..self
        }
    }

    pub fn with_max_base_delay(self, max_base_delay: Duration) -> Self {
        Self {
            max_base_delay,
            ..self
        }
    }

    pub fn with_max_retries(self, max_retries: u32) -> Self {
        Self {
            max_retries,
            ..self
        }
    }

    pub fn build(self) -> RetryBackoff {
        RetryBackoff {
            min_base_delay: self.min_base_delay,
            max_base_delay: self.max_base_delay,
            max_attempts: self.max_retries,
            cur_attempt: 0,
        }
    }
}

pub struct RetryBackoff {
    min_base_delay: Duration,
    max_base_delay: Duration,
    max_attempts: u32,
    cur_attempt: u32,
}

impl Iterator for RetryBackoff {
    type Item = Duration;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cur_attempt == self.max_attempts {
            return None;
        }
        let base_delay = (self
            .min_base_delay
            .saturating_mul(2u32.saturating_pow(self.cur_attempt)))
        .min(self.max_base_delay);
        let jitter =
            Duration::try_from_secs_f64(base_delay.as_secs_f64() * rng().random_range(0.0..=1.0))
                .unwrap_or(Duration::MAX);
        let delay = base_delay + jitter;
        self.cur_attempt += 1;
        Some(delay)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoffs() {
        let backoffs: Vec<_> = RetryBackoffBuilder::default()
            .with_max_retries(6)
            .build()
            .collect();

        assert_eq!(backoffs.len(), 6);
        assert!(backoffs[0] >= Duration::from_millis(100));
        assert!(backoffs[0] <= Duration::from_millis(200));

        assert!(backoffs[1] >= Duration::from_millis(200));
        assert!(backoffs[1] <= Duration::from_millis(400));

        assert!(backoffs[2] >= Duration::from_millis(400));
        assert!(backoffs[2] <= Duration::from_millis(800));

        assert!(backoffs[3] >= Duration::from_millis(800));
        assert!(backoffs[3] <= Duration::from_millis(1600));

        assert!(backoffs[4] >= Duration::from_millis(1000));
        assert!(backoffs[4] <= Duration::from_millis(2000));

        assert!(backoffs[5] >= Duration::from_millis(1000));
        assert!(backoffs[5] <= Duration::from_millis(2000));
    }
}
