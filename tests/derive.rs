use anyhow::Result;
use bigobject::{BigMap, BigObject, Db};
use serde::{Deserialize, Serialize};

#[derive(Default, BigObject, Serialize, Deserialize)]
struct Data {
    int: i32,
    string: String,
    dict: BigMap<String, MapValue>,
}

#[derive(Serialize, Deserialize, Clone)]
struct MapValue {
    int: i32,
    boolean: bool,
}

#[test]
fn various_patterns() -> Result<()> {
    let dir = tempfile::tempdir()?;
    {
        let db: Db<Data> = Db::open(&dir);
        assert_eq!(0, db.r().int);
        assert_eq!("", db.r().string);
        assert!(db.r().dict.get("foo").is_none());
        db.w().int = 3;
        db.w().string = "abc".to_string();
        db.w().dict.insert(
            "foo".to_string(),
            MapValue {
                int: 5,
                boolean: true,
            },
        );
        assert_eq!(3, db.r().int);
        assert_eq!("abc", db.r().string);
        assert_eq!(5, db.r().dict["foo"].int);
        assert!(db.r().dict["foo"].boolean);
        db.w().dict["foo"].int += 5;
    }
    {
        let db: Db<Data> = Db::open(dir);
        assert_eq!(3, db.r().int);
        assert_eq!("abc", db.r().string);
        assert_eq!(10, db.r().dict["foo"].int);
        assert!(db.r().dict["foo"].boolean);
    }
    Ok(())
}
