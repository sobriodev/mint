//! Helper functions to deal with JSON objects.
//!
//! The module provides a set of free functions to deal with JSON objects which are not a part of
//! `serde_json` dependency but yet useful in terms of this library.

use serde_json::Value as JValue;
use std::ops::ControlFlow;

/// Find JSON pointer complement.
///
/// The function returns a tuple consisting of two elements. The former one is a string which
/// contains pointer complement, the latter is a reference to a JSON value denoted by the last
/// token which exists within the input object.
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
///         "inspections": {
///         "date": "2020-01-05"
///         }
///     }
///     ]
///  });
///
/// let (complement, value) = pointer_complement(&john, "/cars_owned/0/inspections/mandatory");
///
/// assert_eq!("/mandatory", complement);
/// assert!(value.is_object() && value["date"].is_string());
/// ```
#[must_use]
pub fn pointer_complement<'a>(jvalue: &'a JValue, pointer: &str) -> (String, &'a JValue) {
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
        ControlFlow::Continue(c) => c,
        ControlFlow::Break(b) => b,
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
///         "inspections": {
///         "date": "2020-01-05"
///         }
///     }
///     ]
///  });
///
/// let (complement, value) = pointer_complement_mut(&mut john, "/cars_owned/0/inspections/mandatory");
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
) -> (String, &'a mut JValue) {
    let (complement, _) = pointer_complement(jvalue, pointer);
    let pointer_mut = jvalue
        .pointer_mut(pointer.trim_end_matches(&complement))
        .unwrap();
    (complement, pointer_mut)
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
    fn test_json() -> JValue {
        json!({
            "name": "John Doe",
            "cars_owned": [{
                "name": "Ford Mustang",
                "age": 5,
                "inspections": {
                    "date": "2020-01-05"
                }
            }
            ]
        })
    }

    /* -------------------------- */
    /* ---- Test definitions ---- */
    /* -------------------------- */

    #[rstest]
    #[case::empty_string("")]
    #[case::single_slash("/")]
    #[case::double_slash("//")]
    #[case::missing_slash("name")]
    fn invalid_pointer_produces_same_string_with_input_json(
        #[case] pointer: &str,
        test_json: JValue,
    ) {
        let output = pointer_complement(&test_json, pointer);
        assert_eq!(output.0, pointer);
        assert_eq!(*output.1, test_json);
    }

    #[rstest]
    #[case::empty_string("")]
    #[case::single_slash("/")]
    #[case::double_slash("//")]
    #[case::missing_slash("name")]
    fn invalid_pointer_produces_same_string_with_mut_input_json(
        #[case] pointer: &str,
        test_json: JValue,
    ) {
        let mut test_json_copy = test_json.clone();
        let output = pointer_complement_mut(&mut test_json_copy, pointer);
        assert_eq!(output.0, pointer);
        assert_eq!(*output.1, test_json);
    }

    #[rstest]
    #[case("/name/invalid_key", "/invalid_key", &test_json()["name"])]
    #[case("/cars_owned/0/invalid_key", "/invalid_key", &test_json()["cars_owned"][0])]
    #[case("/cars_owned/1", "/1", &test_json()["cars_owned"])]
    #[case("/cars_owned/0/5", "/5", &test_json()["cars_owned"][0])]
    #[case("/cars_owned/1/", "/1/", &test_json()["cars_owned"])]
    #[case("/cars_owned////", "////", &test_json()["cars_owned"])]
    #[case("/cars_owned/@?", "/@?", &test_json()["cars_owned"])]
    fn partially_valid_pointer_produces_complement_with_corresponding_json_value(
        #[case] pointer: &str,
        #[case] complement: &str,
        #[case] jvalue: &JValue,
        test_json: JValue,
    ) {
        let output = pointer_complement(&test_json, pointer);
        assert_eq!(output.0, complement);
        assert_eq!(*output.1, *jvalue);
    }

    #[rstest]
    #[case("/name/invalid_key", "/invalid_key", &test_json()["name"])]
    #[case("/cars_owned/0/invalid_key", "/invalid_key", &test_json()["cars_owned"][0])]
    #[case("/cars_owned/1", "/1", &test_json()["cars_owned"])]
    #[case("/cars_owned/0/5", "/5", &test_json()["cars_owned"][0])]
    #[case("/cars_owned/1/", "/1/", &test_json()["cars_owned"])]
    #[case("/cars_owned////", "////", &test_json()["cars_owned"])]
    #[case("/cars_owned/@?", "/@?", &test_json()["cars_owned"])]
    fn partially_valid_pointer_produces_complement_with_corresponding_mut_json_value(
        #[case] pointer: &str,
        #[case] complement: &str,
        #[case] jvalue: &JValue,
        mut test_json: JValue,
    ) {
        let output = pointer_complement_mut(&mut test_json, pointer);
        assert_eq!(output.0, complement);
        assert_eq!(*output.1, *jvalue);
    }

    #[rstest]
    #[case("/name", &test_json()["name"])]
    #[case("/cars_owned", &test_json()["cars_owned"])]
    #[case("/cars_owned/0", &test_json()["cars_owned"][0])]
    #[case("/cars_owned/0/age", &test_json()["cars_owned"][0]["age"])]
    #[case("/cars_owned/0/inspections/date", &test_json()["cars_owned"][0]["inspections"]["date"])]
    fn valid_pointer_produces_empty_string_with_corresponding_json_value(
        #[case] pointer: &str,
        #[case] jvalue: &JValue,
        test_json: JValue,
    ) {
        let output = pointer_complement(&test_json, pointer);
        assert_eq!("", output.0);
        assert_eq!(*output.1, *jvalue);
    }

    #[rstest]
    #[case("/name", &test_json()["name"])]
    #[case("/cars_owned", &test_json()["cars_owned"])]
    #[case("/cars_owned/0", &test_json()["cars_owned"][0])]
    #[case("/cars_owned/0/age", &test_json()["cars_owned"][0]["age"])]
    #[case("/cars_owned/0/inspections/date", &test_json()["cars_owned"][0]["inspections"]["date"])]
    fn valid_pointer_produces_empty_string_with_corresponding_mut_json_value(
        #[case] pointer: &str,
        #[case] jvalue: &JValue,
        mut test_json: JValue,
    ) {
        let output = pointer_complement(&mut test_json, pointer);
        assert_eq!("", output.0);
        assert_eq!(*output.1, *jvalue);
    }
}
