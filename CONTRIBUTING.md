# Submitting a Pull Request

Each Pull Request will be checked against the following rules:

- `cla/signed` - all commit authors need to sign a CLA. This can be done using our [CLA sign form](http://ravendb.net/contributors/cla/sign).

- `commit/whitespace` - all changed files cannot contain TABs inside them. Before doing any work we suggest executing our `git_setup.cmd`. This will install a git pre-commit hook that will normalize all whitespaces during commits.

- `commit/message/conventions` - all commit messages (except in merge commits) must contain an issue number from our [YouTrack](http://issues.hibernatingrhinos.com) e.g. 'RavenDB-1234 Fixed issue with something'

- `tests` - this executes `build.cmd Test` on our CI to check if no constraints were violated
