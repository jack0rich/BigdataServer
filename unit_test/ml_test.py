import torch
import torch.nn as nn
import torch.optim as optim
import torchvision.datasets as datasets
import torchvision.transforms as transforms
from torch.utils.data import DataLoader
import asyncio
import json
from app.services import MLflowAPIClient  # 假设 MLflowAPIClient 在 mlflow_client.py 中


# 定义 MLP 模型
class MLP(nn.Module):
    def __init__(self, input_size, hidden_size, output_size):
        super(MLP, self).__init__()
        self.fc1 = nn.Linear(input_size, hidden_size)
        self.relu = nn.ReLU()
        self.fc2 = nn.Linear(hidden_size, output_size)

    def forward(self, x):
        x = self.fc1(x)
        x = self.relu(x)
        x = self.fc2(x)
        return x


async def train_and_log_mlflow():
    client = MLflowAPIClient()
    experiment_name = "MLP_Classification"
    experiment = await client.create_experiment(experiment_name)
    experiment_id = experiment.get("experiment_id")
    run = await client.create_run(experiment_id, run_name="MLP_Run")
    run_id = run["run"]["info"]["run_id"]

    # 记录超参数
    input_size = 784  # 28x28 MNIST 图片
    hidden_size = 128
    output_size = 10
    lr = 0.001
    batch_size = 64
    epochs = 5
    await client.log_param(run_id, "input_size", input_size)
    await client.log_param(run_id, "hidden_size", hidden_size)
    await client.log_param(run_id, "output_size", output_size)
    await client.log_param(run_id, "learning_rate", lr)
    await client.log_param(run_id, "batch_size", batch_size)
    await client.log_param(run_id, "epochs", epochs)

    # 加载数据
    transform = transforms.Compose([transforms.ToTensor(), transforms.Normalize((0.5,), (0.5,))])
    train_dataset = datasets.MNIST(root="./data", train=True, transform=transform, download=True)
    train_loader = DataLoader(train_dataset, batch_size=batch_size, shuffle=True)

    model = MLP(input_size, hidden_size, output_size)
    criterion = nn.CrossEntropyLoss()
    optimizer = optim.Adam(model.parameters(), lr=lr)

    # 训练模型
    for epoch in range(epochs):
        running_loss = 0.0
        for i, (images, labels) in enumerate(train_loader):
            images = images.view(-1, 28 * 28)
            optimizer.zero_grad()
            outputs = model(images)
            loss = criterion(outputs, labels)
            loss.backward()
            optimizer.step()
            running_loss += loss.item()

        avg_loss = running_loss / len(train_loader)
        print(f"Epoch [{epoch + 1}/{epochs}], Loss: {avg_loss:.4f}")
        await client.log_metric(run_id, "loss", avg_loss, step=epoch)

    # 保存模型
    model_path = "mlp_model.pth"
    torch.save(model.state_dict(), model_path)
    with open("mlp_model.json", "w") as f:
        json.dump({"input_size": input_size, "hidden_size": hidden_size, "output_size": output_size}, f)

    # 记录模型
    await client.log_model(run_id, model_json=json.dumps({"path": model_path}))
    print("Training completed and logged to MLflow!")

    await client.close()


# 运行异步任务
asyncio.run(train_and_log_mlflow())
