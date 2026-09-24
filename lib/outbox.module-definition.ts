import {
  ConfigurableModuleBuilder,
  type ConfigurableModuleAsyncOptions,
  type ModuleMetadata,
  type Provider,
  type Type,
} from '@nestjs/common';
import type { OutboxModuleOptions } from './interfaces/outbox-module-options.interface.js';
import type { OutboxTransport } from './transports/outbox.transport.js';
import { LOCAL_TRANSPORT } from './outbox.constants.js';

/**
 * The top level of both `forRoot()` and `forRootAsync()`. Classes Nest instantiates go
 * only here, never in the async factory's result, because the providers must be known
 * when the module is defined. Instances may go here, or in the factory's result when
 * they are built from injected configuration.
 */
export interface OutboxModuleStructure {
  /**
   * Named transports, each an `OutboxTransport` class or instance. `local` (the
   * `@OnOutboxMessage()` handlers) is built in.
   */
  transports?: Record<string, Type<OutboxTransport> | OutboxTransport>;
  /** Modules whose exports the transport classes inject (`forRootAsync()` has its own `imports`). */
  imports?: ModuleMetadata['imports'];
  /** Default `true`. */
  isGlobal?: boolean;
}

/** `forRoot()`'s options: everything at the top level, where `transports` may be classes. */
export type OutboxModuleRootOptions = Omit<OutboxModuleOptions, 'transports'> & OutboxModuleStructure;

/** What a class passed to `forRootAsync({ useClass })` implements. */
export interface OutboxOptionsFactory {
  createOutboxOptions(): OutboxModuleOptions | Promise<OutboxModuleOptions>;
}

/**
 * What `forRootAsync()` takes: `transports` classes (or instances), `imports` and
 * `isGlobal` at the top level, and one of `useFactory`, `useClass` or `useExisting` for the
 * rest of the options.
 */
export type OutboxModuleAsyncOptions = ConfigurableModuleAsyncOptions<
  OutboxModuleOptions,
  'createOutboxOptions'
> &
  OutboxModuleStructure;

/** The configured transports by name, instantiated (internal). */
export const OUTBOX_TRANSPORTS = Symbol('OUTBOX_TRANSPORTS');

export const {
  ConfigurableModuleClass,
  MODULE_OPTIONS_TOKEN: OUTBOX_MODULE_OPTIONS,
  OPTIONS_TYPE,
} = new ConfigurableModuleBuilder<OutboxModuleOptions>({ moduleName: 'Outbox' })
  .setClassMethodName('forRoot')
  .setFactoryMethodName('createOutboxOptions')
  .setExtras<OutboxModuleStructure>(
    { isGlobal: true, transports: undefined, imports: undefined },
    (definition, { isGlobal, transports, imports }) => ({
      ...definition,
      global: isGlobal,
      imports: [...(definition.imports ?? []), ...(imports ?? [])],
      providers: [...(definition.providers ?? []), ...transportProviders(transports)],
    }),
  )
  .build();

/**
 * The transport providers. Top-level values are checked when the module is defined; the
 * async factory's result is checked at startup, by `fromFactory()`.
 */
function transportProviders(transports: NonNullable<OutboxModuleStructure['transports']> = {}): Provider[] {
  for (const [name, transport] of Object.entries(transports)) {
    assertTransportName(name);
    if (!isClass(transport) && typeof transport?.publish !== 'function') {
      throw new TypeError(
        `OutboxModule: transports.${name} must be an OutboxTransport class or an object with publish()`,
      );
    }
  }

  const topLevel = Object.keys(transports);
  const classes = [...new Set(Object.values(transports).filter(isClass))];
  return [
    ...classes,
    {
      provide: OUTBOX_TRANSPORTS,
      inject: [OUTBOX_MODULE_OPTIONS, ...classes],
      useFactory: (options: OutboxModuleOptions | undefined, ...instances: OutboxTransport[]) => ({
        ...Object.fromEntries(
          Object.entries(transports).map(([name, transport]) => [
            name,
            isClass(transport) ? instances[classes.indexOf(transport)] : transport,
          ]),
        ),
        ...fromFactory(options, topLevel),
      }),
    },
  ];
}

/**
 * The `transports` the async factory returned, checked at startup: instances only (classes
 * go at the top level), and no name set in both places. `forRoot()` never has them in its
 * options value.
 */
function fromFactory(options: OutboxModuleOptions | undefined, topLevel: string[]): Record<string, OutboxTransport> {
  if (options && 'store' in options) {
    throw storeOptionError();
  }

  const { transports = {} } = options ?? {};
  for (const [name, transport] of Object.entries(transports)) {
    assertTransportName(name);
    assertInstance(
      `transports.${name}`,
      transport,
      (value) => typeof value.publish === 'function',
      'an OutboxTransport instance or an object with publish()',
    );
    if (topLevel.includes(name)) {
      throw setTwiceError(`transports.${name}`);
    }
  }

  return transports;
}

/** The store is registered, not configured: a leftover `store` option fails instead of being ignored. */
export function storeOptionError(): Error {
  return new Error(
    'OutboxModule: `store` is not an option. Implement OutboxStore and OutboxInboxStore in a provider ' +
      'that injects OutboxStorage and calls `storage.registerSource({ messages: this, inbox: this })` in ' +
      'its constructor.',
  );
}

function assertInstance<T>(option: string, value: T, valid: (value: T) => boolean, expected: string) {
  if (isClass(value)) {
    throw new Error(
      `OutboxModule: the forRootAsync() factory returned a class as \`${option}\` (${value.name}). ` +
        'Classes go at the top level of forRootAsync(), next to useFactory or useClass, where Nest ' +
        'instantiates them; the factory returns instances.',
    );
  }

  if (!value || typeof value !== 'object' || !valid(value)) {
    throw new TypeError(`OutboxModule: \`${option}\` returned by the forRootAsync() factory must be ${expected}`);
  }
}

function assertTransportName(name: string) {
  if (name === LOCAL_TRANSPORT) {
    throw new Error(
      `OutboxModule: "${LOCAL_TRANSPORT}" is the built-in in-process transport; ` +
        'give the transport in `transports` another name.',
    );
  }
}

function setTwiceError(option: string): Error {
  return new Error(
    `OutboxModule: \`${option}\` is set both at the top level of forRootAsync() and in the ` +
      'options its factory returns. Set it in one place.',
  );
}

function isClass(value: unknown): value is Type {
  return typeof value === 'function';
}
