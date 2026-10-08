import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
    stages: [
        { duration: '5s', target: 0 },
        { duration: '5s', target: 100 },
        { duration: '10s', target: 100 },
        { duration: '5s', target: 0 },
    ],
};

export default function () {
    const res = http.get('http://20.244.71.173/health');
    check(res, {
        'status is 200': (r) => r.status === 200,
    });
    sleep(1);
}