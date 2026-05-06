# API Reference

The public API is small: `fan` maps a function over inputs and returns results in order,
`fan_iter` streams them as they arrive, `submit`/`attach` run detached Batch jobs, `init`
and `sync` do setup, and `Config` holds everything in `.braid/config.toml`. All are
importable from the top-level `braid` package.

## Dispatch

::: braid.core.fan

::: braid.core.fan_iter

## Detached jobs

::: braid.detach.submit

::: braid.detach.attach

::: braid.detach.status

::: braid.detach.JobHandle

::: braid.detach.JobStatus

## Setup

::: braid.api.init

::: braid.api.sync

## Configuration

::: braid.config.Config
    options:
      show_if_no_docstring: true
      members_order: source

::: braid.config.load_config

## Errors

All braid exceptions subclass `braid.errors.BraidError`.

::: braid.errors.ConfigError

::: braid.errors.PayloadTooLargeError

::: braid.errors.LayerBuildError

::: braid.errors.JobTimeoutError

::: braid.errors.JobFailedError

::: braid.errors.WorkerError

::: braid.errors.ThrottleError
