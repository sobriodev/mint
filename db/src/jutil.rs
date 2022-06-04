//! Helper functions to deal with JSON objects.
//!
//! The module provides a set of free functions to deal with JSON objects which are not a part of
//! `serde_json` dependency but yet useful in terms of this library.

use crate::error::{CustomKind, Error, Result};
use serde_json::{Map, Value as JValue};
use std::ops::ControlFlow;

// JSON pointer complement tuple representations.
type Complement<'a> = (String, &'a JValue);
type ComplementMut<'a> = (String, &'a mut JValue);

/// Find JSON pointer complement.
///
/// The function returns a tuple consisting of two elements. The former one is a string which
/// contains pointer complement, the latter is a reference to a JSON value denoted by the last
/// token which exists within the input object.
///
/// # Errors
/// The function returns a custom error in case invalid pointer string was passed.
///
/// # Examples
/// ```
/// use serde_json::{Value, json};
/// use db::jutil::pointer_complement;
///
/// let john = json!({
///     "name": "John Doe",
///     "age": 43,
///     "cars_owned": [{
///         "name": "Ford Mustang",
///         "age": 5,
///         "last_inspection": {
///             "date": "2020-01-05"
///         }
///     }
///     ]
///  });
///
/// let (complement, value) = pointer_complement(&john, "/cars_owned/0/last_inspection/mandatory").unwrap();
///
/// assert_eq!("/mandatory", complement);
/// assert!(value.is_object() && value["date"].is_string());
/// ```
pub fn pointer_complement<'a>(jvalue: &'a JValue, pointer: &str) -> Result<Complement<'a>> {
    if pointer.is_empty() {
        return Ok((pointer.to_string(), jvalue));
    } else if !pointer.starts_with('/') {
        return Err(Error::custom_err(
            CustomKind::Json,
            &format!("Pointer '{}' does not have valid syntax", pointer),
        ));
    }

    let cf = pointer
        .split('/')
        .skip(1)
        .map(|token| format!("/{}", token))
        .try_fold((pointer.to_string(), jvalue), |acc, token| {
            match acc.1.pointer(&token) {
                Some(child) => {
                    ControlFlow::Continue((acc.0.trim_start_matches(&token).to_string(), child))
                }
                None => ControlFlow::Break(acc),
            }
        });

    match cf {
        ControlFlow::Continue(c) => Ok(c),
        ControlFlow::Break(b) => Ok(b),
    }
}

/// Find JSON pointer complement (mutable).
///
/// This is mutable version of the [`pointer_complement`] function.
/// The function is currently not as fast as its non-mutable counterpart because it traverses
/// the input JSON two times to obtain a mutable reference.
///
/// # Panics
/// Should never panic. If the function panics then it should be considered as a bug inside
/// the function's implementation.
///
/// # Errors
/// The function returns a custom error in case invalid pointer string was passed.
///
/// # Examples
/// ```
/// use serde_json::{Value, json};
/// use db::jutil::pointer_complement_mut;
///
/// let mut john = json!({
///     "name": "John Doe",
///     "age": 43,
///     "cars_owned": [{
///         "name": "Ford Mustang",
///         "age": 5,
///         "last_inspection": {
///             "date": "2020-01-05"
///         }
///     }
///     ]
///  });
///
/// let (complement, value) = pointer_complement_mut(&mut john, "/cars_owned/0/last_inspection/mandatory").unwrap();
///
/// assert_eq!("/mandatory", complement);
/// assert!(value.is_object() && value["date"].is_string());
///
/// // Do something useful with returned mutable reference e.g. create a JSON structure which is
/// // represented by complement string.
/// ```
pub fn pointer_complement_mut<'a>(
    jvalue: &'a mut JValue,
    pointer: &str,
) -> Result<ComplementMut<'a>> {
    let (complement, _) = pointer_complement(jvalue, pointer)?;

    let pointer_mut = jvalue
        .pointer_mut(pointer.trim_end_matches(&complement))
        .unwrap();

    Ok((complement, pointer_mut))
}

// Wrap a JSON value
fn wrap_value(tokens: &[String], value: JValue) -> JValue {
    tokens.iter().rfold(value, |acc, token| {
        let mut map = Map::new();
        map.insert(token.to_string(), acc);
        JValue::Object(map)
    })
}

/// Incorporate a JSON structure into another.
///
/// The function computes pointer complement which is not part of the parent object, transforming it
/// into a JSON structure which points to the child object. An error is returned in a case the
/// parent object denoted by the last existing token is neither an array nor pointer.
///
/// The function does not allow to replace existing object keys or array indices.
///
/// # Panics
/// Should never panic. If the function panics then it should be considered as a bug
/// inside the function's implementation.
///
/// # Errors
/// The function may return a number of custom library errors.
///
/// # Examples
/// ```
/// use serde_json::{Value, json};
/// use db::jutil::incorporate_into;
///
/// let mut parent = json!({
///     "foo": "bar"
/// });
/// let child = json!({
///     "bar": "baz"
/// });
///
/// incorporate_into(&mut parent, "/child/object", child);
/// let bar = parent.pointer("/child/object/bar").unwrap();
/// assert_eq!(bar, "baz");
/// ```
pub fn incorporate_into(parent: &mut JValue, pointer: &str, child: JValue) -> Result<()> {
    let (complement, parent) = pointer_complement_mut(parent, pointer)?;
    if complement.is_empty() {
        return Err(Error::custom_err(
            CustomKind::Json,
            &format!("Pointer '{}' already exists", pointer),
        ));
    }

    let tokens: Vec<String> = complement
        .split('/')
        .skip(1)
        .map(|token| token.replace("~1", "/").replace("~0", "~"))
        .collect();

    if parent.is_object() {
        let wrapped = wrap_value(&tokens[1..], child);
        parent
            .as_object_mut()
            .unwrap()
            .insert(tokens[0].clone(), wrapped);
    } else if parent.is_array() {
        let wrapped = wrap_value(&tokens, child);
        parent.as_array_mut().unwrap().push(wrapped);
    } else {
        return Err(Error::custom_err(
            CustomKind::Json,
            &format!(
                "Cannot incorporate since the value pointed by '{}' is neither an array nor object",
                pointer.trim_end_matches(&complement)
            ),
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::*;
    use serde_json::json;

    /* ------------------ */
    /* ---- Fixtures ---- */
    /* ------------------ */

    #[fixture]
    fn test_json_john() -> JValue {
        json!({
            "name": "John Doe",
            "cars_owned": [{
                "name": "Ford Mustang",
                "age": 5,
                "last_inspection": {
                    "date": "2020-01-05"
                }
            }
            ],
            // Special keys
            "": "foo",
            "/": "bar",
            "~": "qux",
            "\"": "quux"
        })
    }

    #[fixture]
    fn test_json_alice() -> JValue {
        json!({
            "name": "Alice Wright",
            "age": 21,
            "hobbies": ["books", "sport", "shopping"]
        })
    }

    /* -------------------------- */
    /* ---- Test definitions ---- */
    /* -------------------------- */

    #[rstest]
    #[case("invalid_pointer")]
    fn pointer_complement_produces_error_when_invalid_pointer_is_passed(
        #[case] pointer: &str,
        mut test_json_john: JValue,
    ) {
        // Immutable version
        pointer_complement(&test_json_john, pointer).unwrap_err();

        // Mutable version
        pointer_complement_mut(&mut test_json_john, pointer).unwrap_err();
    }

    #[rstest]
    fn pointer_complement_returns_empty_string_with_input_json_when_empty_pointer_was_passed(
        test_json_john: JValue,
    ) {
        // Immutable version
        let output = pointer_complement(&test_json_john, "").unwrap();
        assert_eq!(output.0, "");
        assert_eq!(*output.1, test_json_john);

        // Mutable version
        let mut json_mut = test_json_john.clone();
        let output_mut = pointer_complement_mut(&mut json_mut, "").unwrap();
        assert_eq!(output_mut.0, "");
        assert_eq!(*output_mut.1, test_json_john);
    }

    #[rstest]
    #[case("/invalid")]
    #[case("/invalid/key")]
    #[case("/invalid/key/0")]
    fn pointer_complement_returns_same_string_with_input_json_when_non_existing_pointer_is_passed(
        #[case] pointer: &str,
        test_json_john: JValue,
    ) {
        // Immutable version
        let output = pointer_complement(&test_json_john, pointer).unwrap();
        assert_eq!(output.0, pointer);
        assert_eq!(*output.1, test_json_john);

        // Mutable version
        let mut json_mut = test_json_john.clone();
        let output_mut = pointer_complement_mut(&mut json_mut, pointer).unwrap();
        assert_eq!(output_mut.0, pointer);
        assert_eq!(*output_mut.1, test_json_john);
    }

    #[rstest]
    #[case("/name/invalid_key", "/invalid_key", &test_json_john()["name"])]
    #[case("/cars_owned/0/invalid_key", "/invalid_key", &test_json_john()["cars_owned"][0])]
    #[case("/cars_owned/1", "/1", &test_json_john()["cars_owned"])]
    #[case("/cars_owned/2/", "/2/", &test_json_john()["cars_owned"])]
    fn pointer_complement_produces_complement_with_corresponding_json_value_when_partially_existing_pointer_is_passed(
        #[case] pointer: &str,
        #[case] expected_complement: &str,
        #[case] expected_value: &JValue,
        test_json_john: JValue,
    ) {
        // Immutable version
        let output = pointer_complement(&test_json_john, pointer).unwrap();
        assert_eq!(output.0, expected_complement);
        assert_eq!(*output.1, *expected_value);

        // Mutable version
        let mut json_mut = test_json_john.clone();
        let output_mut = pointer_complement_mut(&mut json_mut, pointer).unwrap();
        assert_eq!(output_mut.0, expected_complement);
        assert_eq!(*output_mut.1, *expected_value);
    }

    #[rstest]
    #[case("/", &test_json_john()[""])]
    #[case("/~1", &test_json_john()["/"])]
    #[case("/~0", &test_json_john()["~"])]
    #[case("/\"", &test_json_john()["\""])]
    fn pointer_complement_produces_empty_string_with_corresponding_json_value_when_special_key_is_passed(
        #[case] pointer: &str,
        #[case] expected_value: &JValue,
        test_json_john: JValue,
    ) {
        // Immutable version
        let output = pointer_complement(&test_json_john, pointer).unwrap();
        assert_eq!("", output.0);
        assert_eq!(*output.1, *expected_value);

        // Mutable version
        let mut json_mut = test_json_john.clone();
        let output_mut = pointer_complement_mut(&mut json_mut, pointer).unwrap();
        assert_eq!(output_mut.0, "");
        assert_eq!(*output_mut.1, *expected_value);
    }

    #[rstest]
    #[case("/", &test_json_john()[""])]
    #[case("/name", &test_json_john()["name"])]
    #[case("/cars_owned", &test_json_john()["cars_owned"])]
    #[case("/cars_owned/0", &test_json_john()["cars_owned"][0])]
    #[case("/cars_owned/0/age", &test_json_john()["cars_owned"][0]["age"])]
    #[case("/cars_owned/0/last_inspection/date", &test_json_john()["cars_owned"][0]["last_inspection"]["date"])]
    fn pointer_complement_produces_empty_string_with_corresponding_json_value_when_existing_pointer_is_passed(
        #[case] pointer: &str,
        #[case] expected_value: &JValue,
        test_json_john: JValue,
    ) {
        let output = pointer_complement(&test_json_john, pointer).unwrap();
        assert_eq!("", output.0);
        assert_eq!(*output.1, *expected_value);

        // Mutable version
        let mut json_mut = test_json_john.clone();
        let output_mut = pointer_complement_mut(&mut json_mut, pointer).unwrap();
        assert_eq!(output_mut.0, "");
        assert_eq!(*output_mut.1, *expected_value);
    }

    #[rstest]
    fn wrap_value_returns_same_object_when_no_tokens_are_passed(test_json_john: JValue) {
        let value = test_json_john.clone();
        let wrapped = wrap_value(&[], value);
        assert_eq!(wrapped, test_json_john);
    }

    #[rstest]
    fn wrap_value_returns_wrapped_json(test_json_john: JValue) {
        let value = test_json_john.clone();
        let keys = vec![
            "key1".to_string(),
            "key2".to_string(),
            "".to_string(),
            "0".to_string(),
            "/".to_string(),
        ];

        let wrapped = wrap_value(&keys, value);
        let obtained = wrapped.pointer("/key1/key2//0/~1").unwrap();
        assert_eq!(test_json_john, *obtained);
    }

    #[rstest]
    #[case("")]
    #[case("/")]
    #[case("/~")]
    #[case("/name")]
    #[case("/cars_owned/0")]
    #[case("/cars_owned/0/age")]
    fn incorporate_into_produces_error_when_existing_pointer_is_passed(
        #[case] pointer: &str,
        mut test_json_john: JValue,
        test_json_alice: JValue,
    ) {
        incorporate_into(&mut test_json_john, pointer, test_json_alice).unwrap_err();
    }

    #[rstest]
    #[case("/name")]
    #[case("/cars_owned/0/name")]
    #[case("/cars_owned/0/last_inspection/date")]
    fn incorporate_into_produces_error_when_last_existing_token_is_neither_array_nor_object(
        #[case] pointer: &str,
        mut test_json_john: JValue,
        test_json_alice: JValue,
    ) {
        incorporate_into(&mut test_json_john, pointer, test_json_alice).unwrap_err();
    }

    #[rstest]
    #[case("/alice")]
    #[case("/cars_owned/0/alice")]
    #[case("/cars_owned/0/last_inspection/alice")]
    #[case("/cars_owned/0/last_inspection/alice/is/an/incorporated/object")]
    #[case("/cars_owned/0/last_inspection/alice/is/an//object")]
    #[case("/cars_owned/0/last_inspection/alice/is/an/~1/~0/0/object")]
    fn incorporate_into_injects_object_when_parent_is_object(
        #[case] pointer: &str,
        mut test_json_john: JValue,
        test_json_alice: JValue,
    ) {
        incorporate_into(&mut test_json_john, pointer, test_json_alice.clone()).unwrap();
        assert_eq!(*test_json_john.pointer(pointer).unwrap(), test_json_alice);
    }

    #[rstest]
    #[case("/cars_owned/0/alice")]
    #[case("/cars_owned/0/alice/~0/~1/0")]
    fn incorporate_into_injects_object_when_parent_is_array(
        #[case] pointer: &str,
        mut test_json_john: JValue,
        test_json_alice: JValue,
    ) {
        incorporate_into(&mut test_json_john, pointer, test_json_alice.clone()).unwrap();
        assert_eq!(*test_json_john.pointer(pointer).unwrap(), test_json_alice);
    }
}
