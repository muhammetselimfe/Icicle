import { MessageSquare, ExternalLink } from 'lucide-react';

export default function AiHelpButton() {
    const openInChatGPT = () => {
        const schemaUrl = `${window.location.origin}/ai.sql.txt`;
        const prompt = `Please open ${schemaUrl} and help me write a ClickHouse SQL query based on the schema and examples provided there. \nNow write a SQL query for the following task: `;
        const encodedPrompt = encodeURIComponent(prompt);
        const url = `https://chatgpt.com/?prompt=${encodedPrompt}`;
        window.open(url, '_blank');
    };

    return (
        <button
            onClick={openInChatGPT}
            className="flex items-center gap-2 px-4 py-2 bg-gradient-to-r from-green-600 to-teal-600 text-white rounded-lg font-medium hover:from-green-700 hover:to-teal-700 transition-all cursor-pointer"
        >
            <MessageSquare size={18} />
            Ask ChatGPT
            <ExternalLink size={14} />
        </button>
    );
}

