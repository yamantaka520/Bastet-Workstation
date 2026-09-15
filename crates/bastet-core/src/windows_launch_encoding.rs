//! Encoding primitives for an explicit CreateProcessW backend. These functions
//! do not authorize or launch anything, and must never be used with cmd.exe.
//! Inputs are UTF-16 code units so Windows paths need not be lossy UTF-8.

use std::io;

const MAX_COMMAND_UNITS: usize = 32_767;
// Application resource budget, not a claim about every Windows environment API.
const MAX_ENVIRONMENT_UNITS: usize = 32_767;

fn invalid() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        "invalid native launch encoding",
    )
}

/// Always quote each CRT-style argv item. Backslashes before quotes and before
/// the closing quote are doubled. No shell expansion or raw arguments exist.
pub fn windows_command_line(executable: &[u16], arguments: &[Vec<u16>]) -> io::Result<Vec<u16>> {
    if executable.is_empty() || executable.iter().any(|unit| matches!(*unit, 0 | 34)) {
        return Err(invalid());
    }
    let mut output = Vec::new();
    append(&mut output, 34, 1, MAX_COMMAND_UNITS)?;
    for unit in executable {
        append(&mut output, *unit, 1, MAX_COMMAND_UNITS)?;
    }
    append(&mut output, 34, 1, MAX_COMMAND_UNITS)?;
    for argument in arguments {
        append(&mut output, 32, 1, MAX_COMMAND_UNITS)?;
        append(&mut output, 34, 1, MAX_COMMAND_UNITS)?;
        let mut slashes = 0usize;
        for unit in argument {
            match *unit {
                0 => return Err(invalid()),
                92 => {
                    slashes = slashes.checked_add(1).ok_or_else(invalid)?;
                }
                unit => {
                    let count = if unit == 34 {
                        slashes
                            .checked_mul(2)
                            .and_then(|n| n.checked_add(1))
                            .ok_or_else(invalid)?
                    } else {
                        slashes
                    };
                    append(&mut output, 92, count, MAX_COMMAND_UNITS)?;
                    append(&mut output, unit, 1, MAX_COMMAND_UNITS)?;
                    slashes = 0;
                }
            }
        }
        append(
            &mut output,
            92,
            slashes.checked_mul(2).ok_or_else(invalid)?,
            MAX_COMMAND_UNITS,
        )?;
        append(&mut output, 34, 1, MAX_COMMAND_UNITS)?;
    }
    append(&mut output, 0, 1, MAX_COMMAND_UNITS)?;
    Ok(output)
}

/// Builds a complete explicit Unicode environment, never an inherited one.
/// Keys are restricted to the ASCII names supported by our environment policy;
/// case-insensitive duplicates and hidden drive-current-directory entries fail.
pub fn windows_environment_block(entries: &[(String, Vec<u16>)]) -> io::Result<Vec<u16>> {
    let mut ordered: Vec<_> = entries.iter().collect();
    for (key, value) in &ordered {
        if key.is_empty()
            || !key
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
            || value.contains(&0)
        {
            return Err(invalid());
        }
    }
    ordered.sort_by_cached_key(|(key, _)| key.to_ascii_uppercase());
    if ordered
        .windows(2)
        .any(|pair| pair[0].0.eq_ignore_ascii_case(&pair[1].0))
    {
        return Err(invalid());
    }
    let mut output = Vec::new();
    for (key, value) in ordered {
        for unit in key
            .encode_utf16()
            .chain(std::iter::once(61))
            .chain(value.iter().copied())
            .chain(std::iter::once(0))
        {
            append(&mut output, unit, 1, MAX_ENVIRONMENT_UNITS)?;
        }
    }
    if output.is_empty() {
        append(&mut output, 0, 1, MAX_ENVIRONMENT_UNITS)?;
    }
    append(&mut output, 0, 1, MAX_ENVIRONMENT_UNITS)?;
    Ok(output)
}

fn append(output: &mut Vec<u16>, unit: u16, count: usize, limit: usize) -> io::Result<()> {
    let length = output
        .len()
        .checked_add(count)
        .filter(|length| *length <= limit)
        .ok_or_else(invalid)?;
    output.resize(length, unit);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn wide(value: &str) -> Vec<u16> {
        value.encode_utf16().collect()
    }

    #[test]
    fn command_quotes_spaces_empty_quotes_and_trailing_slashes() {
        let encoded = windows_command_line(
            &wide(r"C:\Program Files\provider.exe"),
            &[
                wide(""),
                wide("a b"),
                wide("a\"b"),
                wide("ends\\"),
                wide("\\\""),
            ],
        )
        .unwrap();
        assert_eq!(
            String::from_utf16(&encoded[..encoded.len() - 1]).unwrap(),
            "\"C:\\Program Files\\provider.exe\" \"\" \"a b\" \"a\\\"b\" \"ends\\\\\" \"\\\\\\\"\""
        );
        assert_eq!(encoded.last(), Some(&0));
    }

    #[test]
    fn preserves_non_bmp_and_unpaired_utf16_without_lossy_conversion() {
        let argument = vec![0xd83d, 0xde00, 0xd800];
        let encoded =
            windows_command_line(&wide("fixture.exe"), std::slice::from_ref(&argument)).unwrap();
        assert!(encoded
            .windows(argument.len())
            .any(|window| window == argument));
        assert_eq!(
            windows_environment_block(&[("VALUE".into(), argument.clone())]).unwrap(),
            [wide("VALUE="), argument, vec![0, 0]].concat()
        );
    }

    #[test]
    fn rejects_nul_invalid_executable_and_expansion_over_budget() {
        assert!(windows_command_line(&[], &[]).is_err());
        assert!(windows_command_line(&wide("a\"b"), &[]).is_err());
        assert!(windows_command_line(&wide("fixture.exe"), &[vec![0]]).is_err());
        assert!(
            windows_command_line(&wide("fixture.exe"), &[vec![92; MAX_COMMAND_UNITS / 2]]).is_err()
        );
        assert!(windows_command_line(&vec![120; MAX_COMMAND_UNITS - 3], &[]).is_ok());
        assert!(windows_command_line(&vec![120; MAX_COMMAND_UNITS - 2], &[]).is_err());
    }

    #[test]
    fn environment_is_explicit_sorted_and_double_terminated() {
        assert_eq!(windows_environment_block(&[]).unwrap(), vec![0, 0]);
        assert_eq!(
            windows_environment_block(&[("z".into(), wide("last")), ("A".into(), wide("x=y"))])
                .unwrap(),
            wide("A=x=y\0z=last\0\0")
        );
        for entries in [
            vec![("PATH".into(), wide("a")), ("Path".into(), wide("b"))],
            vec![("=C:".into(), wide("hidden"))],
            vec![("".into(), vec![])],
            vec![("VALUE".into(), vec![0])],
            vec![("VALUE".into(), vec![120; MAX_ENVIRONMENT_UNITS])],
        ] {
            assert!(windows_environment_block(&entries).is_err());
        }
    }
}
