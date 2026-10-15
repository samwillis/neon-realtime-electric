# Bundled dependencies

This distribution includes `@durable-streams/client` 0.2.7, Copyright Durable
Streams contributors, licensed under Apache-2.0. Source:
https://github.com/durable-streams/durable-streams/tree/461b40267aabd644558f9b19dbb9507dd5f691cf/packages/client

The bundled reader includes a local change to JSON subscription completion:
`closed` waits for body parsing and asynchronous callbacks, and propagates their
failures. The patch is maintained at
`patches/@durable-streams__client@0.2.7.patch` in this project's source repository.
The package's public API is unchanged.

The reader also includes `@microsoft/fetch-event-source`, Copyright Microsoft
Corporation, licensed under MIT. Source:
https://github.com/Azure/fetch-event-source

MIT License

Copyright (c) Microsoft Corporation.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
