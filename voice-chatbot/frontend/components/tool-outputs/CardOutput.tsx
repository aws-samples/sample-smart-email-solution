import { CardToolOutput } from './types';
import { TOOL_OUTPUT_STYLES } from '@/config/tool-outputs';
import {
  Card,
  CardHeader,
  CardTitle,
  CardDescription,
  CardContent,
  CardFooter,
} from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Separator } from "@/components/ui/separator";

// Helper type guard
const isFooterWithAction = (footer: string | { text: string; action?: { text: string; url: string; } }): 
  footer is { text: string; action?: { text: string; url: string; } } => {
  return typeof footer !== 'string';
};

export const CardOutput = ({ output }: { output: CardToolOutput }) => {
  const styles = TOOL_OUTPUT_STYLES.card;

  return (
    <Card className='w-96 mx-auto border-0 shadow-lg border border-gray-200 rounded-none'>
      <CardHeader>
        {output.content.title && (
          <CardTitle>{output.content.title}</CardTitle>
        )}
        {output.content.description && (
          <CardDescription>{output.content.description}</CardDescription>
        )}
      </CardHeader>
      
      <CardContent className="grid gap-6">
        {/* Handle details (text) */}
        {output.content.details && (
          <div className="grid gap-2">
            {typeof output.content.details === 'string' ? (
              <p className="text-sm text-muted-foreground">{output.content.details}</p>
            ) : (
              <div className="grid gap-4">
                {Object.entries(output.content.details).map(([key, value], index) => (
                  <div key={key} className="grid gap-1">
                    <div className="flex items-center justify-between">
                      <p className="text-sm font-medium">{key}</p>
                      {typeof value === 'string' && value.startsWith('$') ? (
                        <Badge variant="secondary" className="font-bold">{value}</Badge>
                      ) : (
                        <span className="text-sm text-muted-foreground">{value}</span>
                      )}
                    </div>
                    {index < Object.entries(output.content.details as Record<string, string>).length - 1 && (
                      <Separator className="mt-2" />
                    )}
                  </div>
                ))}
              </div>
            )}
          </div>
        )}
        
        {/* Handle image */}
        {output.content.image && (
          <div className="relative aspect-video overflow-hidden rounded-lg">
            <img 
              src={output.content.image} 
              alt={output.content.imageAlt || output.content.title || "Card image"}
              className="object-cover w-full h-full"
            />
          </div>
        )}
        
        {/* Handle video */}
        {output.content.video && (
          <div className="relative aspect-video overflow-hidden rounded-lg bg-black">
            <iframe
              src={output.content.video}
              className="w-full h-full"
              title={output.content.title || "Card video"}
              allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture"
              allowFullScreen
            />
          </div>
        )}
      </CardContent>
      
      {/* Card Footer */}
      {output.content.footer && (
        <CardFooter className="flex items-center justify-between border-t border-gray-200 pt-6">
          {typeof output.content.footer === 'string' ? (
            <p className="text-sm text-muted-foreground">{output.content.footer}</p>
          ) : (
            <>
              <p className="text-sm text-muted-foreground">{output.content.footer.text}</p>
              {isFooterWithAction(output.content.footer) && output.content.footer.action && (
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => {
                    if (output.content.footer && isFooterWithAction(output.content.footer) && output.content.footer.action) {
                      window.open(output.content.footer.action.url, '_blank', 'noopener,noreferrer');
                    }
                  }}
                >
                  {isFooterWithAction(output.content.footer) && output.content.footer.action?.text}
                </Button>
              )}
            </>
          )}
        </CardFooter>
      )}
    </Card>
  );
}; 