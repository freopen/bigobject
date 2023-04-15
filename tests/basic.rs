use anyhow::Result;
use serde::{Deserialize, Serialize};
use tempfile::TempDir;

use bigobject::Db;

#[derive(Serialize, Deserialize, Default, Clone)]
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
            let mut db = db.rw();
            assert_eq!(db.int, 0);
            assert_eq!(db.str, "".to_string());
            db.int = 2;
            db.str = "abc".to_string();
        }
        {
            let db = db.r();
            assert_eq!(db.int, 2);
            assert_eq!(db.str, "abc".to_string());
        }
    }
    {
        let db: Db<SerdeObj> = Db::open(dir.path());
        let db = db.r();
        assert_eq!(db.int, 2);
        assert_eq!(db.str, "abc".to_string());
    }
    Ok(())
}
