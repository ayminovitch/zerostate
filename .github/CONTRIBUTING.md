# Contributing to ZeroState

First off, thank you for considering contributing to ZeroState! 

## Development Setup

1. Clone the repository
2. Run `npm install`
3. Run `npm run test` to verify the test suite passes locally
4. Make your changes in a new branch

## Pull Request Process

1. Ensure all tests pass.
2. Update the README.md with details of changes to the interface, if applicable.
3. Your PR must follow conventional commits for the title.

## Architecture Guidelines

* Do not add blocking logic to the `MeshRouter` fast path.
* Any changes to the `Transport` layer must be carefully profiled for memory leaks or `EBUSY` conditions.
* Always ensure graceful teardown in `ZeroState.stop()`.
