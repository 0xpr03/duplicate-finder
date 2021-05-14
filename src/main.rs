use rayon::ThreadPoolBuilder;
use serde::Serialize;
use sha2::{
    digest::{
        consts::{U32},
        generic_array::GenericArray,
        FixedOutput,
    },
    Digest, Sha256,
};

use std::{
    fs::{OpenOptions},
    io::{Error, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use std::collections::{HashMap};
use walkdir::WalkDir;

type Result<T> = std::result::Result<T, Error>;
type Hash = GenericArray<u8, U32>;
// Hash -> Vec<Files with same hash>
type FileHashMap = HashMap<Hash, Vec<PathBuf>>;

fn main() -> Result<()> {
    let debug = false;
    println!("Hello, world!");

    let args: Vec<String> = std::env::args().collect();
    let path = match args.get(1) {
        Some(v) => v,
        None => {
            eprintln!("Can't find ");
            return Ok(());
        }
    };
    let mut amount: usize = 0;
    let mut spawned: usize = 0;
    let start = Instant::now();
    let map: Arc<Mutex<FileHashMap>> = Arc::new(Mutex::new(HashMap::new()));
    {
        let threadpool = ThreadPoolBuilder::new()
            .build()
            .expect("Can't init threadpool");

        for entry in WalkDir::new(path).follow_links(false) {
            amount += 1;
            let entry = match entry {
                Ok(v) => v,
                Err(e) => {
                    if debug {
                        eprintln!("{}", e);
                    }
                    continue;
                }
            };
            let map_c = map.clone();
            let path = entry.into_path();
            spawned += 1;
            threadpool.spawn(move || match hash_file(&path) {
                Ok(v) => {
                    let mut guard = map_c.lock().expect("Can't lock map!");
                    if let Some(v) = guard.get_mut(&v) {
                        v.push(path);
                    } else {
                        guard.insert(v, vec![path]);
                    }
                }
                Err(e) => {
                    if debug {
                        eprintln!("Can't process {:?}: {}", path, e);
                    }
                }
            });
        }

        drop(threadpool);
    }

    loop {
        let val = Arc::strong_count(&map);
        if val == 1 {
            break;
        } else {
            let done = spawned - val;
            let progress = (done * 100) / spawned;
            let elapsed = start.elapsed().as_millis();
            let eta = (elapsed as u128 / done as u128) * val as u128;
            let time_s = eta / 1000;
            let time_ms = eta - (time_s * 1000);
            println!(
                "{}% Indexed {} files of {} ETA {}s:{}ms",
                progress, done, spawned, time_s, time_ms
            );
            std::thread::sleep(Duration::from_millis(500));
        }
    }
    let end = start.elapsed();
    let lock = Arc::try_unwrap(map).expect("Arc still has multiple owners");
    let map = lock.into_inner().expect("Can't lock map!");
    println!(
        "Hashed {} entries in {}ms., ({} files, {}ms/file)",
        map.len(),
        end.as_millis(),
        amount,
        end.as_millis() / amount as u128
    );

    // folders -> duplicate files (any source)
    let mut duplicate_folders: HashMap<Vec<PathBuf>, Vec<PathBuf>> = HashMap::new();
    for (_k, mut files) in map.into_iter() {
        let mut folders: Vec<PathBuf> = files
            .iter()
            .map(|f| f.parent().unwrap().to_owned())
            .collect();
        // sort,otherwise our hash key is random
        folders.sort();
        if files.len() > 1 {
            if let Some(v) = duplicate_folders.get_mut(&folders) {
                // just take the first file with same hash
                v.push(files.pop().unwrap());
            } else {
                // just take the first file with same hash
                duplicate_folders.insert(folders, vec![files.pop().unwrap()]);
            }
        }
    }

    let serializable: Vec<_> = duplicate_folders
        .into_iter()
        .map(|(folders, files)| DeserializedObject { folders, files })
        .collect();
    let file_p = "data.json";
    let parsed = serde_json::to_string(&serializable).expect("Can't serialize");
    let mut file = std::fs::File::create(file_p).expect("create failed");
    file.write_all(parsed.as_bytes())?;
    file.flush()?;

    println!("Finished, results written to {}",file_p);

    Ok(())
}

#[derive(Debug, Serialize)]
struct DeserializedObject {
    folders: Vec<PathBuf>,
    files: Vec<PathBuf>,
}

fn hash_file(path: &Path) -> Result<Hash> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(false)
        .create(false)
        .open(path)?;
    let mut sha256 = Sha256::new();
    std::io::copy(&mut file, &mut sha256)?;
    let hash: Hash = sha256.finalize_fixed();
    Ok(hash)
}
