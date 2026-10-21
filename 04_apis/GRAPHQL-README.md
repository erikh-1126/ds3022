# GraphQL APIs (GitHub example)

GraphQL is a query language for APIs. Instead of many REST endpoints that each return a
fixed JSON shape, a GraphQL API exposes **one endpoint** and you describe **exactly the
data you want** in the request body.

Example script: [`08_github_graphql_api.py`](08_github_graphql_api.py)

## REST vs. GraphQL

| | REST | GraphQL |
|---|---|---|
| Endpoints | Many (`/repos/{owner}/{repo}/issues`, `/users/{user}`, ...) | One (`https://api.github.com/graphql`) |
| HTTP method | GET / POST / PATCH / DELETE describe the action | Always POST at GitHub; the body says `query` (read) or `mutation` (write) |
| Response shape | Fixed by the server, often more than you need | Matches the fields you asked for |
| Related data | Multiple requests (issue, then its author, then its labels) | One request with nested fields |
| Pagination | `page` / `per_page` params | Cursors: `first`, `after`, `pageInfo { endCursor hasNextPage }` |
| Errors | HTTP status codes | Usually HTTP 200 with an `errors` key in the JSON |
| Rate limit | 5,000 requests/hour | 5,000 **points**/hour (cost depends on the query) |

## Anatomy of a request

Every call is a POST with a JSON body holding the query text and its variables:

```python
payload = {
  "query": QUERY,                                   # the GraphQL text
  "variables": {"cursor": None},                   # None = start at the first page
}
r = httpx.post("https://api.github.com/graphql", headers=HEADERS, json=payload)
```

The query itself, fetching closed issues from [pandas](https://github.com/pandas-dev/pandas):

```graphql
query($cursor: String) {                                    # declare variables + types
  repository(owner: "pandas-dev", name: "pandas") {         # entry point (a "query" field)
    issues(states: CLOSED, first: 100, after: $cursor) {    # a connection, with arguments
      totalCount
      pageInfo { hasNextPage endCursor }                    # pagination info
      nodes {                                               # the actual Issue objects
        number
        title
        closedAt
        author { login }                                    # nested object
      }
    }
  }
}
```

The repo is typed directly into the query. `$cursor` is a variable because it changes on
every page. To see what a query costs, add `rateLimit { cost remaining resetAt }` at the
top level, alongside `repository`.

Key terms:

- **Query / Mutation**: a read or a write operation.
- **Object**: a type with fields (`Repository`, `Issue`, `User`).
- **Field**: a piece of data on an object. Some fields take arguments (`issues(states: CLOSED)`).
- **Scalar**: a leaf value (`String`, `Int`, `DateTime`, `Boolean`).
- **Enum**: a fixed set of values (`IssueState` is `OPEN` or `CLOSED`).
- **Connection**: a paginated list. You read results from `nodes` (or `edges { node }`) and
  page with `pageInfo`.
- **`!`**: a non-null (required) type, e.g. `String!`.

## Pagination with cursors

1. Request `first: 100` and include `pageInfo { hasNextPage endCursor }`.
2. If `hasNextPage` is true, send the same query again with `after: <endCursor>`.
3. Repeat until `hasNextPage` is false.

## Handling errors

A malformed query, a misspelled field or a missing repo still returns **HTTP 200**. Always
check the body:

```python
data = r.json()
if "errors" in data:
  print(data["errors"])
```

## Finding the fields you can request

The schema is strongly typed and fully documented. To find out what you can ask for:

1. Start at an **entry point** in [Queries](https://docs.github.com/en/graphql/reference/queries),
   e.g. `repository`, `user`, `organization`, `search`.
2. Open the **object** it returns in [Objects](https://docs.github.com/en/graphql/reference/objects),
   e.g. [`Repository`](https://docs.github.com/en/graphql/reference/objects#repository) lists
   every field, including connections like `issues`, `pullRequests`, `stargazers`.
3. Follow nested types, e.g. [`Issue`](https://docs.github.com/en/graphql/reference/objects#issue).
4. Look up allowed argument values in [Enums](https://docs.github.com/en/graphql/reference/enums)
   (e.g. `IssueState`, `IssueOrderField`) and
   [Input objects](https://docs.github.com/en/graphql/reference/input-objects) (e.g. `IssueOrder`).

The fastest way to explore is the **[GraphQL Explorer](https://docs.github.com/en/graphql/overview/explorer)**.
Sign in with GitHub, then build queries interactively with autocomplete (Ctrl+Space) and
inline docs. Paste the working query into Python afterward.

You can also download the full schema
([public schema](https://docs.github.com/en/graphql/overview/public-schema)) or ask the API
itself with an introspection query (`{ __type(name: "Issue") { fields { name } } }`).

## Documentation

GitHub GraphQL:
- [GitHub GraphQL API overview](https://docs.github.com/en/graphql)
- [Forming calls with GraphQL](https://docs.github.com/en/graphql/guides/forming-calls-with-graphql) (auth, endpoint, query structure)
- [Using pagination](https://docs.github.com/en/graphql/guides/using-pagination-in-the-graphql-api)
- [Rate limits and query limits](https://docs.github.com/en/graphql/overview/rate-limits-and-query-limits-for-the-graphql-api)
- [Comparing GitHub's REST and GraphQL APIs](https://docs.github.com/en/rest/about-the-rest-api/comparing-githubs-rest-api-and-graphql-api)
- Reference: [Queries](https://docs.github.com/en/graphql/reference/queries) ·
  [Mutations](https://docs.github.com/en/graphql/reference/mutations) ·
  [Objects](https://docs.github.com/en/graphql/reference/objects) ·
  [Enums](https://docs.github.com/en/graphql/reference/enums) ·
  [Input objects](https://docs.github.com/en/graphql/reference/input-objects) ·
  [Scalars](https://docs.github.com/en/graphql/reference/scalars)

GraphQL in general:
- [Introduction to GraphQL](https://graphql.org/learn/)
- [Serving over HTTP](https://graphql.org/learn/serving-over-http/) (GET vs. POST)
- [Introspection](https://graphql.org/learn/introspection/)
