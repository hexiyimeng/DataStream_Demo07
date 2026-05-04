import type { NodeSpec } from '../types';

export type ParsedPortType = {
  raw: string;
  container: string;
  dtype?: string | null;
};

export function parsePortType(type: string): ParsedPortType {
  const raw = String(type || '').trim();
  const match = raw.match(/^([A-Za-z0-9_]+)\[([A-Za-z0-9_]+)\]$/);
  if (!match) return { raw, container: raw, dtype: null };
  return { raw, container: match[1], dtype: match[2].toLowerCase() };
}

export function formatPortType(type: string): string {
  const parsed = parsePortType(type);
  if (parsed.container === 'DASK_ARRAY') {
    return parsed.dtype ? `Dask Array - ${parsed.dtype}` : 'Dask Array';
  }
  if (parsed.container === 'MODEL') {
    return parsed.dtype ? `Model - ${parsed.dtype}` : 'Model';
  }
  return parsed.raw;
}

function mismatchMessage(sourceType: string, targetType: string): string {
  return `Type mismatch: output is ${sourceType}, input requires ${targetType}. Insert an explicit converter or use a compatible node.`;
}

export function canConnectPorts(sourceType: string, targetType: string): {
  ok: boolean;
  reason?: string;
} {
  const source = parsePortType(sourceType);
  const target = parsePortType(targetType);

  if (source.container !== target.container) {
    return { ok: false, reason: mismatchMessage(sourceType, targetType) };
  }

  if (source.container === 'MODEL') {
    const sourceProvider = source.dtype ?? null;
    const targetProvider = target.dtype ?? null;
    if (targetProvider === null || targetProvider === 'any') return { ok: true };
    if (sourceProvider === null || sourceProvider === 'any') {
      return { ok: false, reason: mismatchMessage(sourceType, targetType) };
    }
    return sourceProvider === targetProvider
      ? { ok: true }
      : { ok: false, reason: mismatchMessage(sourceType, targetType) };
  }

  if (source.container !== 'DASK_ARRAY') {
    if (source.raw === target.raw || source.raw === '*' || target.raw === '*') {
      return { ok: true };
    }
    return { ok: false, reason: mismatchMessage(sourceType, targetType) };
  }

  const sourceDtype = source.dtype ?? null;
  const targetDtype = target.dtype ?? null;

  if (targetDtype === null || targetDtype === 'any') {
    return { ok: true };
  }

  if (targetDtype === 'same') {
    if (sourceDtype === null || sourceDtype === 'any') {
      return { ok: false, reason: mismatchMessage(sourceType, targetType) };
    }
    return { ok: true };
  }

  if (sourceDtype === null || sourceDtype === 'any' || sourceDtype === 'same') {
    return { ok: false, reason: mismatchMessage(sourceType, targetType) };
  }

  if (sourceDtype === targetDtype) {
    return { ok: true };
  }

  return { ok: false, reason: mismatchMessage(sourceType, targetType) };
}

export function resolveNodeOutputTypes(
  nodeSpec: NodeSpec | undefined,
  values: Record<string, unknown> | undefined,
): string[] {
  if (!nodeSpec?.output) return [];

  const nodeType = nodeSpec.type || nodeSpec.name;
  if (nodeType === 'DaskTypeCast') {
    const target = values?.target_dtype;
    if (typeof target === 'string' && target) {
      return [`DASK_ARRAY[${target}]`];
    }
    return ['DASK_ARRAY[float32]'];
  }

  return nodeSpec.output;
}
