import { Button } from "@/components/ui/button";
import { Card, CardHeader, CardTitle } from "@/components/ui/card";

interface ButtonToolOutput {
  type: 'button';
  content: {
    title: string;
    buttonText: string;
    id: string;
  };
}

interface ButtonOutputProps {
  output: ButtonToolOutput;
  websocket: WebSocket | null | undefined;
}

export const ButtonOutput = ({ output, websocket }: ButtonOutputProps) => {
  const handleClick = () => {
    console.log('Button clicked, websocket state:', websocket?.readyState);
    
    if (websocket && websocket.readyState === WebSocket.OPEN) {
      const message = {
        event: {
          ui_interaction: {
            type: "button_click",
            buttonId: output.content.id,
            action: "click"
          }
        }
      };
      
      console.log('Sending message:', message);
      websocket.send(JSON.stringify(message));
    } else {
      console.log('WebSocket not available or not connected');
    }
  };

  return (
    <Card className="w-96 mx-auto border-0 shadow-lg border border-gray-200 rounded-none">
      <CardHeader>
        <CardTitle>{output.content.title}</CardTitle>
        <Button 
          onClick={handleClick}
          className="w-full mt-4"
        >
          {output.content.buttonText}
        </Button>
      </CardHeader>
    </Card>
  );
}; 