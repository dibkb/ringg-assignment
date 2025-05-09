from datetime import datetime
import time
class ScoreRec:
    __slots__ = ('user_id', 'game_id', 'score', 'timestamp')
    def __init__(self, data: dict):
        self.user_id = data['user_id']
        self.game_id = data['game_id']
        self.score = int(data['score'])
        if data.get('timestamp') is None:
            self.timestamp = time.time()
        elif isinstance(data['timestamp'], datetime):
            self.timestamp = data['timestamp'].timestamp()
        elif isinstance(data['timestamp'], str):
            dt = datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00'))
            self.timestamp = dt.timestamp()
        else:
            self.timestamp = float(data['timestamp'])

    def to_dict(self):
        return {
            'user_id': self.user_id,
            'game_id': self.game_id,
            'score': self.score,
            'timestamp': self.timestamp
        }

class Leader:
    __slots__ = ('user_id', 'score')
    def __init__(self, user_id: str, score: int):
        self.user_id = user_id
        self.score = score

class RankInfo:
    __slots__ = ('user_id', 'score', 'rank', 'percentile')
    def __init__(self, user_id: str, score: int, rank: int, total: int):
        self.user_id = user_id
        self.score = score
        self.rank = rank
        self.percentile = 100 * (1 - (rank - 1) / total)