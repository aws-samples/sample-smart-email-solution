export interface BaseToolOutput {
  type: string;
  content: {
    title?: string;
    description?: string;
    [key: string]: any;
  };
}

export interface TextToolOutput extends BaseToolOutput {
  type: 'text';
  content: {
    title?: string;
    [key: string]: any;
  };
}

export interface CardToolOutput extends BaseToolOutput {
  type: 'card';
  content: {
    title?: string;
    description?: string;
    details?: Record<string, string> | string;
    image?: string;
    imageAlt?: string;
    footer?: string | {
      text: string;
      action?: {
        text: string;
        url: string;
      };
    };
  };
}

export interface ImageToolOutput extends BaseToolOutput {
  type: 'image';
  content: {
    title?: string;
    description?: string;
    url: string;
    alt?: string;
  };
}

export interface VideoToolOutput extends BaseToolOutput {
  type: 'video';
  content: {
    title?: string;
    description?: string;
    url: string;
  };
}

export interface PdfToolOutput extends BaseToolOutput {
  type: 'pdf';
  content: {
    title?: string;
    description?: string;
    url: string;
  };
}

export interface ButtonToolOutput {
  type: 'button';
  content: {
    title: string;
    buttonText: string;
    id: string;
  };
}

export interface BargeinToolOutput extends BaseToolOutput {
  type: 'barge_in';
  content: {
    title?: string;
    description?: string;
    status: string;
    [key: string]: any;
  };
}

export interface AppToolOutput {
  type: 'app';
  appName: string;
  props?: Record<string, any>;
}

export type ToolOutput =
  | TextToolOutput
  | ImageToolOutput
  | VideoToolOutput
  | PdfToolOutput
  | ButtonToolOutput
  | BargeinToolOutput
  | AppToolOutput
  | CardToolOutput; 