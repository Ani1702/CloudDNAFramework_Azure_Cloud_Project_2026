import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
    vus: 10,
    duration: '60s',
};

export default function () {
    const res = http.get('http://20.244.71.173/health');
    check(res, {
        'service still responding': (r) => r.status === 200 || r.status === 503,
        'response received': (r) => r.status !== 0,
    });
    sleep(1);
}