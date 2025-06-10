// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use crate::encode::record::array::{
    ArrayAppend, ArrayOptions, BinaryArrayBuilder, CheckedArrayAppend, FixedSizeBinaryArrayBuilder,
    PrimitiveArrayBuilder, StringArrayBuilder, UInt8ArrayBuilder,
};

struct AdaptiveStructBuilder {
    builders: Vec<BuilderInner>,
}

enum BuilderInner {
    Binary(BinaryArrayBuilder),
    FixedSizeBinary(FixedSizeBinaryArrayBuilder),
    String(StringArrayBuilder),
    UInt8(UInt8ArrayBuilder),
    // ...
    // ...
}

impl AdaptiveStructBuilder {
    fn new() -> Self {
        Self {
            builders: vec![BuilderInner::String(StringArrayBuilder::new(
                ArrayOptions {
                    ..Default::default()
                },
            ))],
        }
    }

    fn field_builder<T>(&mut self, i: usize) -> Option<&mut T>
    where
        T: ArrayAppend + 'static,
    {
        if let Some(builder) = self.builders.get_mut(i) {
            match builder {
                BuilderInner::String(b) => b.as_any_mut().downcast_mut(),
                BuilderInner::Binary(b) => b.as_any_mut().downcast_mut(),
                BuilderInner::UInt8(b) => b.as_any_mut().downcast_mut(),
                BuilderInner::FixedSizeBinary(_) => {
                    // this type of builder must be accessed through `checked_field_builder`
                    None
                }
            }
        } else {
            // index out of bounds
            None
        }
    }

    fn checked_field_builder<T>(&mut self, i: usize) -> Option<&mut T>
    where
        T: CheckedArrayAppend + 'static,
    {
        if let Some(builder) = self.builders.get_mut(i) {
            match builder {
                BuilderInner::String(_) | BuilderInner::Binary(_) | BuilderInner::UInt8(_) => {
                    // this type of builder must be accessed through `field_builder`
                    None
                }
                BuilderInner::FixedSizeBinary(b) => b.as_any_mut().downcast_mut(),
            }
        } else {
            // index out of bounds
            None
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_smoke_TODO_deletehtis() {
        let mut struct_b = AdaptiveStructBuilder::new();
        let str_b = struct_b.field_builder::<StringArrayBuilder>(0);
        assert!(str_b.is_some())
    }
}
