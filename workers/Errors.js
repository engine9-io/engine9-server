class ErrorWithMetadata extends Error {
  constructor(
    message,
    metadata,
    stack = '',
  ) {
    super(message);

    this.name = this.constructor.name;
    this.message = message;
    this.success = false;
    this.metadata = metadata;

    if (stack) {
      this.stack = stack;
    } else {
      Error.captureStackTrace(this, this.constructor);
      if (this.metadata) this.stack = `Error Metadata: ${JSON.stringify(this.metadata, null, 4)}\n${this.stack}`;
    }
  }
}
module.exports = { ErrorWithMetadata };
