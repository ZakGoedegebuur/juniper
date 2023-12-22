# WIP
Currently this threadpool is a WIP and other thread pool solutions are recommended

## Performance:
When performing this operation on a slice of 100,000,000 u32s I observed a 9* performance increase using my Ryzen 5 3600 (6 hardware cores, 12 virtual cores):
```zig
for (0..1000) |i| {
    if (i % 2 == 0) {
        num.* *= 3;
    } else {
        num.* /= 3;
    }
}
```
-Doptimize was set to ReleaseFast. For smaller operations or those which involve large numbers of tasks, I expect this improvement to go down.

## Installing:
