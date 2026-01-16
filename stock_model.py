import torch
import torch.nn as nn
import math

class PositionalEncoding(nn.Module):
    def __init__(self, d_model, max_len=5000):
        super(PositionalEncoding, self).__init__()
        pe = torch.zeros(max_len, d_model)
        position = torch.arange(0, max_len, dtype=torch.float).unsqueeze(1)
        div_term = torch.exp(torch.arange(0, d_model, 2).float() * (-math.log(10000.0) / d_model))
        pe[:, 0::2] = torch.sin(position * div_term)
        pe[:, 1::2] = torch.cos(position * div_term)
        pe = pe.unsqueeze(0) # (1, max_len, d_model)
        self.register_buffer('pe', pe)

    def forward(self, x):
        # x: (Batch, Seq_Len, d_model)
        x = x + self.pe[:, :x.size(1), :]
        return x

class StockProbabilityModel(nn.Module):
    def __init__(self, input_dim=12, d_model=128, nhead=4, num_layers=2, future_window=60, num_bins=100):
        super(StockProbabilityModel, self).__init__()
        self.future_window = future_window
        self.num_bins = num_bins
        self.d_model = d_model
        
        # 1. Input Projection
        self.input_proj = nn.Linear(input_dim, d_model)
        
        # 2. Positional Encoding
        self.pos_encoder = PositionalEncoding(d_model)
        
        # 3. Transformer Encoder
        encoder_layers = nn.TransformerEncoderLayer(d_model=d_model, nhead=nhead, dim_feedforward=512, dropout=0.1, batch_first=True)
        self.transformer_encoder = nn.TransformerEncoder(encoder_layers, num_layers=num_layers)
        
        # 4. Decoder / Output Head
        # We take the entire sequence output of the transformer (Batch, 60, d_model)
        # Flatten it to (Batch, 60 * d_model) and project to (Batch, 60 * 100)
        # Or we can just use a dense layer on each time step?
        # Since we are predicting a future Map from a Past Sequence, the mapping isn't necessarily 1-to-1 timestep wise.
        # But a common approach for "Seq-to-Map" is to flatten the encoded representation.
        
        self.output_head = nn.Sequential(
            nn.Flatten(),
            nn.Linear(60 * d_model, 2048),
            nn.ReLU(),
            nn.Dropout(0.2),
            nn.Linear(2048, future_window * num_bins)
        )
        
        self.sigmoid = nn.Sigmoid()

    def forward(self, x):
        # x shape: (Batch, Sequence=60, Features=7)
        
        # Project features
        x = self.input_proj(x)
        x = x * math.sqrt(self.d_model)
        x = self.pos_encoder(x)
        
        # Transformer encoding
        # output: (Batch, Seq=60, d_model)
        encoded = self.transformer_encoder(x)
        
        # Project to output map
        flat_out = self.output_head(encoded) # (Batch, 6000)
        
        # Reshape to (Batch, 60, 100)
        output = flat_out.view(-1, self.future_window, self.num_bins)
        
        return output
