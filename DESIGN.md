# Design notes

What I built, why I built it that way, what I measured, and what is left. The [README](README.md) says how to run it.

## The idea in one minute

The portal shows a "Similar companies" box. To fill it, the service needs the companies that do similar business.

```
Registry feed  →  Queue (RabbitMQ)  →  Consumer  →  Postgres + pgvector  →  API  →  Portal
messages about    "company-events"      cleans and    one clean row per       "similar to
companies                               saves         company, with its       this company?"
                                                      description as numbers
```

1. The registry sends messages ("company X was added or changed").
2. The **consumer** (my program) cleans each message, combines it with what we already know about that company, and turns the business description into numbers (an *embedding*). Companies with similar descriptions get similar numbers.
3. Everything is saved in **Postgres**. Finding the nearest numbers is done by the **pgvector** extension.
4. The **API** answers "which companies are similar to this one?" by asking Postgres for the nearest ones.

**The hard part is not the similarity search. It is the feed.** It is duplicated, out of order and dirty, so most of the design is about making the saved result correct whatever the feed does.

## Where to find things in the code

| Question | File |
|---|---|
| How is one message cleaned? | [similarco/events.py](similarco/events.py) |
| How are many messages about one company combined? | [similarco/merge.py](similarco/merge.py) |
| How is a batch saved to Postgres? | [similarco/ingest.py](similarco/ingest.py) |
| How does it read the queue, retry and stop safely? | [similarco/consumer.py](similarco/consumer.py) |
| How are descriptions turned into numbers? | [similarco/embedding.py](similarco/embedding.py) |
| How is "similar" found and returned? | [similarco/queries.py](similarco/queries.py), [similarco/api.py](similarco/api.py) |
| What does the database look like? | [similarco/schema.sql](similarco/schema.sql) |
| Where are the tests? | [tests/](tests/) |
| Where do the measurements come from? | [evaluation/](evaluation/) |

## 1. What was wrong with the data

The registry team said they do not check what they send, so I looked at the data before writing any code. I tried to read every line, counted which keys and kinds of value each line has, listed every spelling of the small fields (state, industry), and grouped messages by company. That found everything in the table below, and each row has a test.

| Problem | Example | How many | What the service does |
|---|---|---|---|
| Line cut off in the middle | `{"type": ..., "data": {"companyName": "Bes` | 5 | Set aside with a reason. The same companies have good messages elsewhere. |
| Line is just `null` | `null` | 6 | Set aside. |
| No registration number | company "No Reg Sdn. Bhd." | 7 | Set aside: we cannot tell which company it is. |
| Registration number is a plain number | `119`, company name "X" | 3 | Set aside, not guessed (see assumption 3). |
| Registration number spelled differently | `syn-001988`, ` SYN-001497 ` | 49 | Tidied to `SYN-001988`. Without this there would be 2,049 "companies". |
| Time written in odd ways | ends in `Z` (28), no timezone (14), a big number (30), missing (7) | 79 | Converted to one clock (UTC). Missing means "oldest possible". |
| The same message sent twice | identical lines | 68 | No effect (decision 1). |
| A company with several messages | e.g. SYN-000675 has 3 | 406 | Combined (decision 1). |
| ...arriving in the wrong order | | 65 | The newest wins, not the last to arrive. |
| ...with different details | | 142 | Newest value of each detail wins. |
| Message with almost nothing in it | only a name, or no `data` | 23 | Fills in what it says and leaves the rest alone. |
| State spelled `KUALA LUMPUR`, `kuala lumpur` | | 388 | One spelling for each of the 16 states. |
| One phrase repeated hundreds of times | "Perunding pengurusan untuk PKS" ×100 | 7 | Collapsed to one copy. |
| Company with no description anywhere | | 39 | Still stored and recommended (decision 4). |
| `company-struck-off` messages | company closed down | 8 | Decision 3. |
| Descriptions in Malay | | 28% of messages | Drove the choice of model (decision 5). |
| Few different descriptions | about 170 for 2,000 companies | | They are templates with the city swapped. This shapes the search (decision 6). |

## 2. Decisions

Each decision says what the problem was, what I did, why, and how I know it works.

### 1. Duplicates and wrong order: for each detail, the newest time wins

- **Problem.** The queue can deliver a message twice, and in any order. The saved data must not depend on that.
- **What I did.** Each detail of a company (name, state, description...) keeps the value from the **newest message that mentioned it**. "Newest" means the message's `updatedAt` time, and the message id breaks an exact tie, so every computer picks the same one. The database stores which message each detail came from (`field_versions`), so the rule holds across restarts.
- **Why per detail, not per company.** If a half-empty message replaced the whole record, it would erase good data. This way it only changes what it mentions, and a late old message can still fill a gap.
- **Why not an "already seen" list.** The message id is a fingerprint of the text, so it only catches *exact* copies. A re-sent message with a new time has a different id. The merge rule covers both cases.
- **Proof.** Tests shuffle every real company's messages many times and the answer never changes. The whole feed loaded forwards, backwards and shuffled gives identical rows. Sending the feed a second time changes nothing (`changed=0`).

### 2. Save first, then confirm. Set bad messages aside. Never throw away good ones

- **What I did.** A batch of up to 64 messages is saved in one transaction. Only after the save succeeds do I tell RabbitMQ "got it". If the program crashes, the messages come back, which is harmless because of decision 1.
- **Bad message** (cut off, no number...): saved in the `rejected_messages` table with the reason, then confirmed, so it can never get stuck in a loop.
- **Database or queue trouble:** put the batch back and wait (2 seconds, doubling up to 30).
- **Unexpected error:** retry one message at a time. A message that fails alone *while others in the batch succeed* is set aside as `processing_error`. If *all* fail, the fault is the system (for example a crashed model), so they are put back. A broken model must not turn good data into "rejected" data.
- **Why no dead-letter queue.** The registry team owns `company-events` and declared it with no extra settings, so declaring it differently would fail.
- **Proof.** Tests with a real RabbitMQ cover both behaviours.

### 3. Struck-off companies stay, marked, and are not recommended

A `company-struck-off` message sets `status = struck_off` under the same newest-wins rule, so a later update can bring a company back and an old one cannot. A struck-off company keeps its own page (the brief says users expect every company to be findable) and still shows similar active companies, but it is never recommended to others.

### 4. A company with no description is still recommended

39 companies never have a description. Dropping them would break "no lost companies". They are matched using `"industry: company name"` instead, and flagged `embedding_basis = fallback` so the portal can tell. A company with no description, industry or name is stored with no numbers: its page returns 200 with an empty list, not an error.

### 5. The model: `paraphrase-multilingual-mpnet-base-v2`

- **Problem.** The model turns a description into numbers. About a quarter of the descriptions are in Malay, and an English-only model handles those badly.
- **What I did.** I first picked a smaller multilingual model (MiniLM) after a thin test. Then I tested **eight models** properly, with a rule written *before* the large ones ran: switch only if another model runs at 20+ texts per second on this CPU **and** scores more than 0.03 higher. mpnet was the only one that did both, so I switched.
- **It is used as it comes.** Nothing is trained or fine-tuned: there are no labelled "these companies are similar" examples, and 169 template descriptions would be memorised, not learned.
- **Full table, how it was measured, and the cost of the switch:** section 5.
- **If speed matters more than about 0.04 of quality**, MiniLM is the sensible alternative. Changing model means changing one constant (`EMBEDDING_DIM`), `schema.sql`, and re-loading. The service checks the table when it starts and refuses to run on the wrong size, with the commands to fix it (tested).

### 6. How "similar" is found: exact search, stable tie-break, no index yet

For a company, the API fetches its numbers and asks Postgres for the nearest active companies:

```sql
SELECT ... FROM companies
WHERE registration_number <> :me AND status = 'active' AND embedding IS NOT NULL
ORDER BY embedding <=> :vector, md5(registration_number || :me)
LIMIT :k
```

- `<=>` is cosine distance, and the score returned is `1 - distance` (1.0 means identical).
- **Ties.** 2,000 companies share about 170 descriptions, so a company has dozens of neighbours at distance exactly 0. Ordering them by registration number would put the lowest numbers on every page. The `md5` of the pair gives a stable order that differs between pages: the same page always shows the same companies (tested), but no company is favoured everywhere.
- **No index yet.** At 2,000 companies the search takes 14 to 16 ms inside Postgres and is exact. An index would make it approximate for a saving that does not matter yet. Section 6 shows when that changes (about 10x).

### 7. Keep the numbers inside the database row

This came from measuring, not from design. I first assumed the search would take "a few milliseconds". It took about 70 ms.

- **Why.** Postgres moves any value in a row larger than about 2 kB out to a separate storage area ("TOAST"). Each vector is 3 kB, so every vector lived there and every distance paid an extra lookup.
- **Fix.** One line in `schema.sql` (`SET STORAGE PLAIN`) keeps the vector in the row. The search went from **67–86 ms to 14–16 ms**. A test keeps it that way.
- **Note.** It only applies to rows written after it runs, so an existing database needs `just down`, `just up`, and the feed loaded again.

### 8. What the API says when something goes wrong

| Situation | Reply |
|---|---|
| We have never heard of that company | 404 `company_not_found` |
| Not a `SYN-######` number, or `limit` outside 1–50 | 422 |
| Database not reachable | 503 `database_unavailable`, with `Retry-After` |
| A bug | 500 `internal_error` (details in our log only) |
| A known company with nothing to compare | 200 with an empty list |

Every error has the same shape, `{"error": {"code": ..., "message": ...}}`, so the portal handles them in one place. The "similar" box is secondary to the page, so on a 503 the portal can hide it and carry on. A query is cut off after 3 seconds and a request waits at most 5 seconds for a database connection, so a slow database becomes a 503, not a frozen page.

### 9. Python 3.13, not 3.14

The scaffold pins Python 3.14. The library that runs the model (`onnxruntime`) has no Python 3.14 version, and its newest releases have none for Intel Macs, which is what I develop on. PyTorch has none for Intel Macs either, so `sentence-transformers` was not an option. Python 3.13 with fastembed works on Intel, Apple Silicon and Linux.

## 3. Assumptions

Where the brief left something open, this is what I decided. Each is easy to change in [similarco/events.py](similarco/events.py).

1. **A time with no timezone is Malaysia time (+08:00).** Every other time in the feed is, and the untagged ones follow the same daily sequence.
2. **A missing or unreadable time means "oldest possible".** The company is kept but never overrides anything that has a time.
3. **A plain-number registration number is junk, not a short `SYN-` number.** Padding `119` to `SYN-000119` could overwrite a real company with the name "X".
4. **A missing detail means "no information", not "delete it".** So a detail can be changed but never cleared. The feed has no way to say "cleared", and treating missing as deleted would let every half-empty message erase data.
5. **Times from different sources can be compared.** The feed mixes `ssm-batch`, `partner-api` and `manual-entry`. If their clocks disagree, newest-wins inherits that. I ignore the `source` field.
6. **"Similar" means similar business activity.** Ranking uses the description only. State and industry are shown but do not change the order. The brief says that is enough for a first version.
7. **A struck-off company can come back** if a newer update arrives.
8. **The message id only identifies exact copies**, because it is a fingerprint of the text.

## 4. Testing

`just test` runs 114 tests in about a minute (115 with the real-model test, `just test-all`).

| What | Tests | Proves |
|---|---|---|
| Cleaning and merging | `test_events`, `test_merge`, `test_feed` | every problem in section 1, one case each; same result in any order; exact counts for the real feed (21 set aside, 2,000 companies, 8 struck off, 39 no description) |
| Postgres | `test_pipeline`, `test_api`, `test_schema` | the whole feed loaded, repeated, reversed and shuffled; a failed batch leaves nothing behind; every API status code; wrong-size table refused |
| RabbitMQ | `test_consumer` | a 400-message drain; a bad message does not block the rest; a broken model does not lose messages |
| Models | `test_embedding`, `test_evaluation` | the model test data really comes from the feed |

**Not tested:** two consumers running at the same time (the design locks rows in a fixed order, so it should be safe), a RabbitMQ or Postgres restart in the middle of a run, and heavy load beyond the benchmarks below.

**A lesson from the testing.** One of my tests passed for the wrong reason: a cached queue count always read 0, so "the queue is empty" was trivially true. I noticed because a different check failed, then fixed it and added a check that the helper really sees the 400 messages before the drain. A passing test is not the same as a correct one.

## 5. Going deeper (the optional hour): which model, how fast, how far

### A. Which model is best?

Most descriptions exist in both English and Malay, so I built the test from your feed. I matched **31 Malay descriptions to their English twins** by hand (a test checks every one really is in the feed) and added 12 English ones with no twin as extra wrong answers. Three things are measured, and the **score is their average**:

- **Industry agreement:** of a description's 5 nearest other descriptions, how many have the same industry. (Random guessing gives 0.11.)
- **Cross-language agreement:** the same, but Malay may only be compared with English and the other way round, so a model cannot win by matching shared words.
- **Twin found:** does a Malay description find its English twin among 43 English ones? (counted out of 31)

| Model | Size | Speed (texts/s) | Industry agreement | Cross-language | Twin found | **Score** |
|---|---|---|---|---|---|---|
| Word-counting baseline | | | 0.703 | 0.172 | 4 | 0.335 |
| `bge-small-en-v1.5` (English only; the brief's suggestion) | 0.07 GB | 80 | 0.714 | 0.224 | 6 | 0.377 |
| `potion-multilingual-128M` | 0.51 GB | 10,004 | 0.820 | 0.543 | 28 | 0.756 |
| `paraphrase-multilingual-MiniLM-L12-v2` (my first choice) | 0.22 GB | 112 | 0.849 | 0.753 | 30 | 0.856 |
| **`paraphrase-multilingual-mpnet-base-v2` (chosen)** | 1.00 GB | 34 | **0.871** | **0.805** | **31** | **0.892** |
| `embeddinggemma-300m` | 1.24 GB | 18 | 0.806 | 0.695 | 30 | 0.823 |
| `Qwen3-Embedding-0.6B` (int8) | 1.12 GB | 3 | 0.825 | 0.456 | 26 | 0.706 |
| `multilingual-e5-large` | 2.24 GB | 7 | 0.807 | 0.690 | 31 | 0.832 |

What it shows:

- **mpnet wins under the rule** (0.892 against 0.856). The bigger models did not score higher on this data and run at 3 to 18 texts per second here.
- **The lead is real but small.** On the same queries mpnet beats MiniLM by +0.022 on industry agreement (95% range +0.004 to +0.043) and +0.052 on cross-language (+0.025 to +0.080).
- **`bge-small` treats Malay as close to noise.** Its nearest neighbours for a Malay civil-engineering description were a retail shop and a durian farm. It is fine for English text.
- **What the switch costs.** Vectors are twice the size (768 numbers, 3 kB each), the download is 1.0 GB instead of 0.22 GB, embedding is about 3x slower, and the exact search costs 3 to 5 times more, so an index is needed sooner.

**Limits.** There are only 169 distinct descriptions, all made from templates. I matched the Malay-English pairs myself (one pair, "Online retail of baby products and toys", is not a word-for-word translation). The industry label is a rough proxy. I did not test models above about 2.4 GB or ones that need a GPU. Speeds are single runs on a laptop that is not otherwise idle. None of this proves a model is best on real production data, which would need real behaviour such as clicks.

### B. How fast is the API?

From `just eval-latency` on one Intel MacBook, with Postgres in Docker on the same machine (so it includes Docker's network overhead). 2,000 real companies, 1,000 requests, one server process:

| Clients at once | p50 ms | p95 ms | p99 ms | Requests per second |
|---|---|---|---|---|
| 1 | 21.6 | 32.8 | 44.8 | 44 |
| 8 | 73.9 | 139.7 | 198.5 | 98 |
| 32 | 379.4 | 597.8 | 718.1 | 77 |

*(p50: half of requests are faster than this. p95: 95% are.)* With one client a request takes about 22 ms, of which 14–16 ms is the search inside Postgres. That is fast enough for a panel that loads with the page. The 8 and 32-client rows look like requests queueing on one process; I did not profile further. For more, run several workers.

### C. What happens when it grows?

A synthetic table made from the real numbers, repeated and slightly changed up to 20,000 and 100,000 companies. "Exact duplicates" is the share of rows that repeat a description word for word, as this feed does. "Recall" is how many of the truly nearest results the index found.

| Companies | Exact duplicates | Exact search p50 ms | Index (HNSW) p50 ms | Index recall, `ef_search` 40 → 200 | Index size |
|---|---|---|---|---|---|
| 20,000 | 50% | 157 | 3.0 | 0.825 → 0.833 | 48 MB |
| 20,000 | 0% | 140 | 3.3 | 0.948 → 0.971 | 78 MB |
| 100,000 | 50% | 957 | 3.9 | 0.915 → 0.915 | 292 MB |
| 100,000 | 0% | 901 | 6.9 | 0.882 → 0.941 | 390 MB |

- **Exact search grows in a straight line** with the number of companies (7 to 9 microseconds each). Extrapolating, it would take about 75 ms at 10,000 companies and 150 ms at 20,000. With the earlier smaller model it was 52 ms at 20,000.
- **The index is 40 to 250 times faster** (3 to 7 ms) but approximate: it finds 83% to 97% of the true nearest.
- **Exact duplicates hurt the index.** With duplicates, trying harder (`ef_search` 40 → 200) barely helps; without them it does. This feed is mostly duplicates. A likely fix, not built or measured: index the *distinct descriptions*, find the nearest ones, then expand to their companies.
- **The query has to change to use an index.** My `md5` tie-break stops Postgres using it. The benchmark first picks 50 candidates by distance, then applies the tie-break to those. The service does not do this today.

## 6. What would change at 10x the volume

10x is about 20,000 companies and 25,000 messages. In the order things would break:

1. **Search breaks first.** About 150 ms at 20,000 companies is too slow for a panel that loads with the page. Switch to the HNSW index with the query change above, and check recall on real data before trusting the benchmark. Plan for the duplicate problem. At 100,000 companies the index is 290 to 390 MB, which fits in memory.
2. **The API.** One process handled about 44 requests per second one at a time and about 100 with 8 clients. Run several workers or replicas. Recommendations change slowly, so the portal can also cache each answer for a few minutes.
3. **Embedding.** 26 to 34 descriptions per second on one CPU. The full feed loaded in about 18 seconds because it only has about 170 different descriptions, each embedded once. Real data is more varied: 20,000 different ones would take 10 to 13 minutes, fine for a first load. At 100x, run more consumers or use a GPU or a smaller model.
4. **Saving.** About 140 messages per second with one consumer, and Postgres was not the limit. For more, use bigger batches and several consumers. The merge rule does not depend on order and rows are locked in a fixed order, so this should be safe (untested).
5. **Storage.** Vectors are 3 kB each: 60 MB at 20,000 companies, 3 GB at a million. `rejected_messages` needs a clean-up rule.
6. **Changing the model** at that size needs a re-index command (below), because replaying the whole feed is slow.

## 7. What is left

In rough order of what I would do next:

1. **Judge the model on real behaviour.** Everything in decision 5 uses the industry label and my own translations. With real data I would check which recommendations people click.
2. **Monitoring.** Today there are logs and the `rejected_messages` table. I would track queue length, how long a batch takes, the share of messages set aside by reason (0.8% now, a jump means the registry changed something), any `processing_error` (should be zero), and the API's p95 and error rate.
3. **Two consumers at once, and a RabbitMQ or Postgres restart mid-run.** Not tested yet.
4. **A re-index command** that re-embeds from the `companies` table, so a model change does not need the feed replayed.
5. **Better ranking.** Prefer the same state or industry when similarity ties, vary the results, or learn from clicks.
6. **Explaining a recommendation with an LLM** (the brief's design-only option). Not done. I would send the two descriptions to a small local model and keep the sentence per pair.
7. **Housekeeping.** Retention for `rejected_messages`, a Dockerfile for the two services, CI that runs `just lint` and `just test`, a retry when RabbitMQ is not up yet (the consumer exits today), and API login and caching headers.

## 8. AI tools

I used **Claude Code** (Claude Sonnet 5.5) in the terminal for most of this: looking at the feed, drafting the code and tests, running the model comparison and the benchmarks, and drafting this document and the README. Every number here comes from a command in the repo (`just eval-quality`, `just eval-latency`, `just test`), and the feed counts are checked by tests, so they can be re-run and checked, not taken on trust.

## Glossary

| Word | Plain meaning |
|---|---|
| **Queue** | A conveyor belt of messages. The registry puts messages on it and the consumer takes them off. Here it is called `company-events`. |
| **Consumer** | My program that reads messages from the queue, cleans them and saves them. |
| **Acknowledge** | Telling the queue "I have safely dealt with this message, you can remove it." |
| **Embedding / vector** | A list of numbers that stands for the meaning of a sentence. Similar meaning, nearby numbers. |
| **pgvector** | A Postgres extension that stores these numbers and finds the nearest ones. |
| **Cosine similarity** | A score for how alike two vectors are. 1.0 is identical, near 0 is unrelated. |
| **Message id** | A fingerprint of a message's text. Identical text gives an identical id. |
| **Safe to repeat** | Doing something twice gives the same result as doing it once. |
| **Tie-break** | A rule that decides the order when several results are equally good. |
| **Index (HNSW)** | A shortcut that finds nearby vectors fast but may miss a few. Exact search checks every row. |
| **Recall** | Of the truly nearest results, how many the index found. |
| **p50 / p95** | The time that half / 95% of requests are faster than. |
| **TOAST** | Postgres's separate, slower storage area for large values in a row. |
