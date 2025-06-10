// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use arrow::{
    array::{ArrayRef, StructArray},
    datatypes::{DataType, Field, FieldRef},
};

use crate::encode::record::array::{
    AdaptiveArrayBuilder, ArrayAppend, ArrayBuilder, ArrayOptions, BinaryArrayBuilder,
    CheckedArrayAppend, FixedSizeBinaryArrayBuilder, PrimitiveArrayBuilder, StringArrayBuilder,
    UInt8ArrayBuilder,
};

struct FieldData {
    name: String,
    nullable: bool,
    // TODO support metadata
}

enum FieldBuilder {
    Binary(BinaryArrayBuilder),
    FixedSizeBinary(FixedSizeBinaryArrayBuilder),
    String(StringArrayBuilder),
    UInt8(UInt8ArrayBuilder),
    // ...
    // ...
}

impl<T> From<T> for FieldBuilder
where
    T: ArrayAppend + 'static,
{
    fn from(value: T) -> Self {
        todo!()
    }
}

impl FieldBuilder {
    fn finish(&mut self) -> Option<ArrayRef> {
        match self {
            Self::Binary(b) => b.finish(),
            Self::FixedSizeBinary(b) => b.finish(),
            Self::String(b) => b.finish(),
            Self::UInt8(b) => b.finish(),
        }
    }
}

struct AdaptiveStructBuilder {
    fields: Vec<(FieldData, FieldBuilder)>,
}

impl AdaptiveStructBuilder {
    fn new<T, B>(fields: T) -> Self
    where
        T: IntoIterator<Item = (FieldData, B)>,
        B: Into<FieldBuilder>,
    {
        Self {
            fields: fields
                .into_iter()
                .map(|(data, builder)| (data, builder.into()))
                .collect(),
        }
    }

    fn field_builder<T>(&mut self, i: usize) -> Option<&mut T>
    where
        T: ArrayAppend + 'static,
    {
        if let Some((_, builder)) = self.fields.get_mut(i) {
            match builder {
                FieldBuilder::String(b) => b.as_any_mut().downcast_mut(),
                FieldBuilder::Binary(b) => b.as_any_mut().downcast_mut(),
                FieldBuilder::UInt8(b) => b.as_any_mut().downcast_mut(),
                FieldBuilder::FixedSizeBinary(_) => {
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
        if let Some((_, builder)) = self.fields.get_mut(i) {
            match builder {
                FieldBuilder::String(_) | FieldBuilder::Binary(_) | FieldBuilder::UInt8(_) => {
                    // this type of builder must be accessed through `field_builder`
                    None
                }
                FieldBuilder::FixedSizeBinary(b) => b.as_any_mut().downcast_mut(),
            }
        } else {
            // index out of bounds
            None
        }
    }

    /// TODO should this thing return an option ArrayRef if all the rows are nullable? (A: yes)
    fn finish(&mut self) -> ArrayRef {
        let mut arrays: Vec<(FieldRef, ArrayRef)> = vec![];
        for i in 0..self.fields.len() {
            let (field_data, builder) = &mut self.fields[i];
            if let Some(array) = builder.finish() {
                // TODO -- do we really need an arc here?
                let field = Arc::new(Field::new(
                    &field_data.name,
                    array.data_type().clone(),
                    field_data.nullable,
                ));
                arrays.push((field, array))
            }
        }

        Arc::new(StructArray::from(arrays))
    }
}

#[cfg(test)]
mod test {
    use crate::encode::record::array::{ArrayBuilderConstructor, NoArgs};

    use super::*;

    #[test]
    fn test_get_field() {
        let struct_builder = AdaptiveStructBuilder::new(vec![(
            // TODO it's ugly how we have to specify nullable here twice ...
            FieldData {
                name: "test".to_string(),
                nullable: false,
            },
            StringArrayBuilder::new(ArrayOptions {
                dictionary_options: None,
                nullable: true,
            }),
        )]);
        // check it works for each type of field
    }
}
