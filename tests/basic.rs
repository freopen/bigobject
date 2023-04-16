use anyhow::Result;
use serde::{Deserialize, Serialize};
use tempfile::TempDir;

use bigobject::{BigMap, Db};

#[derive(Debug, Serialize, Deserialize, Default, Clone, PartialEq)]
struct SerdeObj {
    int: i32,
    str: String,
}

#[test]
fn serde_root() -> Result<()> {
    let dir = TempDir::new()?;
    {
        let db: Db<SerdeObj> = Db::open(dir.path());
        {
            let mut write = db.w();
            assert_eq!(write.int, 0);
            assert_eq!(write.str, "".to_string());
            write.int = 2;
            write.str = "abc".to_string();
        }
        {
            let read = db.r();
            assert_eq!(read.int, 2);
            assert_eq!(read.str, "abc".to_string());
        }
    }
    {
        let db: Db<SerdeObj> = Db::open(dir.path());
        let read = db.r();
        assert_eq!(read.int, 2);
        assert_eq!(read.str, "abc".to_string());
    }
    Ok(())
}

#[test]
fn big_map_root() -> Result<()> {
    let test_obj = SerdeObj {
        int: 2,
        str: "def".to_string(),
    };
    let dir = TempDir::new()?;
    {
        let db: Db<BigMap<String, SerdeObj>> = Db::open(dir.path());
        {
            let mut write = db.w();
            assert_eq!(None, write.get("abc"));
            write.insert("abc".to_string(), test_obj.clone());
            assert_eq!(test_obj, write["abc"]);
            write["abc"].int += 1;
            assert_eq!(3, write["abc"].int);
        }
        {
            let read = db.r();
            assert_eq!(3, read["abc"].int);
            assert_eq!(None, read.get("def"));
        }
    }
    {
        let db: Db<BigMap<String, SerdeObj>> = Db::open(dir.path());
        {
            let read = db.r();
            assert_eq!(None, read.get("def"));
            assert_eq!(3, read["abc"].int);
        }
        db.w().clear();
        assert_eq!(None, db.r().get("abc"));
    }
    Ok(())
}
