import { test, expect } from 'e2e';
import { spawnSync } from 'node:child_process';
import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
const project = resolve(fileURLToPath(new URL('..', import.meta.url)));
for (const scenario of ['lifecycle', 'queue', 'memory', 'run', 'errors', 'signals']) {
  test(`isolated HTTP and CLI: ${scenario}`, async () => {
    const result = spawnSync(resolve(project, '../.venv/bin/python'), [resolve(project, 'scenario.py'), scenario], {
      cwd: resolve(project, '..'), encoding: 'utf8', timeout: 45000,
      env: { ...process.env, PYTHONPATH: resolve(project, '../src') },
    });
    expect(result.error).toBeUndefined();
    expect(result.status, result.stderr).toBe(0);
    expect(JSON.parse(result.stdout)).toEqual({scenario, result: 'passed'});
  });
}
