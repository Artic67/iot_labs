from app.entities.agent_data import AgentData
from app.entities.processed_agent_data import ProcessedAgentData

pit_intervals = {
    "normal": {
        "start": 14000, 
        "end": 18000
        },
    "small pits less": {
        "start": 12000, 
        "end": 14000
        },
    "small pits greater": {
        "start": 18000, 
        "end": 20000
        }
}

def process_agent_data(
    agent_data: AgentData,
) -> ProcessedAgentData:
    """
    Process agent data and classify the state of the road surface.
    Parameters:
        agent_data (AgentData): Agent data that containing accelerometer, GPS, and timestamp.
    Returns:
        processed_data_batch (ProcessedAgentData): Processed data containing the classified state of the road surface and agent data.
    """
    # Implement it
    z_acceleration = agent_data.accelerometer.z

    if pit_intervals["normal"]["start"] < z_acceleration <= pit_intervals["normal"]["end"]:
        road_state = "normal"
    elif pit_intervals["small pits less"]["start"] < z_acceleration < pit_intervals["small pits less"]["end"] or pit_intervals["small pits greater"]["start"] < z_acceleration < pit_intervals["small pits greater"]["end"]:
        road_state = "small pits"
    else:
        road_state = "large pits"
    return ProcessedAgentData(road_state=road_state, agent_data=agent_data)